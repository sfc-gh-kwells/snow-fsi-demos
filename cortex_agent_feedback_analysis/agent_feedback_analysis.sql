-- =============================================================================
-- Cortex Agent Feedback Analysis using AI_COMPLETE
-- =============================================================================
-- Analyzes user feedback from the Cortex Agent REST API to:
--   1. Categorize problem types users are experiencing
--   2. Extract actionable insights for agent improvements
--
-- Prerequisites:
--   - MONITOR privilege on the agent object
--   - SNOWFLAKE.CORTEX_USER database role
--   - READ UNREDACTED AI OBSERVABILITY EVENTS TABLE privilege (for full text)
-- =============================================================================

-- Replace these with your actual values
SET database_name = 'PORTFOLIO_AGENT_DB';
SET schema_name = 'DATA';
SET agent_name = 'PORTFOLIO_ASSISTANT_AGENT';

-- =============================================================================
-- 1. View raw feedback
-- =============================================================================
SELECT
    TIMESTAMP AS feedback_time,
    RECORD_ATTRIBUTES:"snow.ai.observability.user.name"::STRING AS user_name,
    VALUE:positive::BOOLEAN AS is_positive,
    VALUE:feedback_message::STRING AS feedback_message,
    VALUE:categories::ARRAY AS user_categories,
    RECORD_ATTRIBUTES:"snow.ai.observability.agent.thread_id"::STRING AS thread_id
FROM TABLE(SNOWFLAKE.LOCAL.GET_AI_OBSERVABILITY_EVENTS(
    $database_name, $schema_name, $agent_name, 'CORTEX AGENT'
))
WHERE RECORD:name = 'CORTEX_AGENT_FEEDBACK'
ORDER BY TIMESTAMP DESC;

-- =============================================================================
-- 2. Categorize feedback using AI_COMPLETE (per-row classification)
-- =============================================================================
CREATE OR REPLACE TEMPORARY TABLE feedback_categorized AS
WITH feedback AS (
    SELECT
        TIMESTAMP AS feedback_time,
        RECORD_ATTRIBUTES:"snow.ai.observability.user.name"::STRING AS user_name,
        VALUE:positive::BOOLEAN AS is_positive,
        VALUE:feedback_message::STRING AS feedback_message,
        VALUE:categories::STRING AS user_categories,
        RECORD_ATTRIBUTES:"snow.ai.observability.agent.thread_id"::STRING AS thread_id
    FROM TABLE(SNOWFLAKE.LOCAL.GET_AI_OBSERVABILITY_EVENTS(
        $database_name, $schema_name, $agent_name, 'CORTEX AGENT'
    ))
    WHERE RECORD:name = 'CORTEX_AGENT_FEEDBACK'
      AND VALUE:feedback_message IS NOT NULL
      AND VALUE:feedback_message::STRING != ''
)
SELECT
    feedback_time,
    user_name,
    is_positive,
    feedback_message,
    user_categories,
    thread_id,
    PARSE_JSON(
        REGEXP_REPLACE(
            AI_COMPLETE(
                'claude-haiku-4-5',
                'You are a feedback classifier for an AI agent. Categorize the following user feedback into exactly ONE primary category and provide a brief explanation. Return valid JSON only with no markdown formatting.

Categories:
- INCORRECT_ANSWER: The agent gave wrong or inaccurate information
- INCOMPLETE_ANSWER: The answer was partially correct but missing key details
- TOOL_FAILURE: A tool (search, analyst, SQL) failed or returned bad results
- HALLUCINATION: The agent made up information not grounded in sources
- SLOW_RESPONSE: Performance or latency complaints
- STALE_DATA: Data sources are outdated or not refreshed
- OFF_TOPIC: Agent did not understand the question or went off-topic
- PERMISSION_ERROR: User could not access data or got authorization errors
- MISSING_CAPABILITY: Agent lacks a feature or data source the user needs
- OTHER: Does not fit other categories

User feedback message: ' || feedback_message || '
User-selected categories: ' || COALESCE(user_categories, 'none') || '

Return ONLY raw JSON, no code fences: {"category": "<CATEGORY>", "explanation": "<brief explanation>"}',
                {'temperature': 0, 'max_tokens': 200}
            ),
            '```[a-z]*\\n?|```', ''
        )
    ) AS classification
FROM feedback;

-- =============================================================================
-- 3. View categorized results
-- =============================================================================
SELECT
    feedback_time,
    user_name,
    is_positive,
    feedback_message,
    user_categories,
    classification:category::STRING AS ai_category,
    classification:explanation::STRING AS ai_explanation
FROM feedback_categorized
ORDER BY feedback_time DESC;

-- =============================================================================
-- 4. Summary: count of issues by AI-assigned category
-- =============================================================================
SELECT
    classification:category::STRING AS problem_category,
    COUNT(*) AS feedback_count,
    SUM(IFF(is_positive = FALSE, 1, 0)) AS negative_count,
    SUM(IFF(is_positive = TRUE, 1, 0)) AS positive_count,
    ROUND(negative_count / NULLIF(feedback_count, 0) * 100, 1) AS negative_pct
FROM feedback_categorized
GROUP BY problem_category
ORDER BY negative_count DESC;

-- =============================================================================
-- 5. Summary: count by user-selected categories (exploded from array)
-- =============================================================================
SELECT
    f.value::STRING AS user_category,
    COUNT(*) AS times_selected
FROM TABLE(SNOWFLAKE.LOCAL.GET_AI_OBSERVABILITY_EVENTS(
    $database_name, $schema_name, $agent_name, 'CORTEX AGENT'
)) e,
LATERAL FLATTEN(input => e.VALUE:categories) f
WHERE e.RECORD:name = 'CORTEX_AGENT_FEEDBACK'
  AND e.VALUE:positive::BOOLEAN = FALSE
GROUP BY user_category
ORDER BY times_selected DESC;

-- =============================================================================
-- 6. Extract actionable insights using AI_COMPLETE on aggregated feedback
-- =============================================================================
WITH all_negative_feedback AS (
    SELECT LISTAGG(
        '- ' || feedback_message || ' [categories: ' || COALESCE(user_categories, 'none') || ']',
        '\n'
    ) WITHIN GROUP (ORDER BY feedback_time DESC) AS combined_feedback
    FROM feedback_categorized
    WHERE is_positive = FALSE
)
SELECT AI_COMPLETE(
    'claude-sonnet-4-6',
    'You are an AI agent improvement advisor. Analyze the following negative user feedback collected from a Cortex Agent and provide actionable recommendations for the agent owner.

Negative feedback items:
' || combined_feedback || '

Provide your analysis in this structure:
1. TOP 3 MOST COMMON PROBLEMS: What patterns do you see?
2. ROOT CAUSES: What might be causing these issues in the agent configuration?
3. RECOMMENDED AGENT UPDATES: Specific changes (e.g., refresh data sources, add Cortex Search services, update semantic models, adjust system prompt, add tools)
4. PRIORITY: Rank recommendations by impact (high/medium/low)

Be specific and actionable.',
    {'temperature': 0.2, 'max_tokens': 2000}
) AS improvement_recommendations
FROM all_negative_feedback;

-- =============================================================================
-- 7. Trend analysis: feedback sentiment over time
-- =============================================================================
SELECT
    DATE_TRUNC('week', TIMESTAMP) AS feedback_week,
    COUNT(*) AS total_feedback,
    SUM(IFF(VALUE:positive::BOOLEAN = TRUE, 1, 0)) AS positive,
    SUM(IFF(VALUE:positive::BOOLEAN = FALSE, 1, 0)) AS negative,
    ROUND(positive / NULLIF(total_feedback, 0) * 100, 1) AS satisfaction_pct
FROM TABLE(SNOWFLAKE.LOCAL.GET_AI_OBSERVABILITY_EVENTS(
    $database_name, $schema_name, $agent_name, 'CORTEX AGENT'
))
WHERE RECORD:name = 'CORTEX_AGENT_FEEDBACK'
GROUP BY feedback_week
ORDER BY feedback_week DESC;
