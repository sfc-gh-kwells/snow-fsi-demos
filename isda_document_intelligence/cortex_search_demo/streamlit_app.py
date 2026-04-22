"""
Cortex Search Demo - Multi-Service
Demonstrates advanced Cortex Search features across multiple services:
- Toggle between different Cortex Search services
- Dynamic filtering by attributes
- Scoring config: numeric boosts and time decays
- Component scores (text_match, cosine_similarity, reranker_score)
- Query latency metrics
- Evaluation metrics (NDCG, Precision, Recall)
"""

import json
import math
import time
import streamlit as st
from snowflake.snowpark.context import get_active_session

# Page config
st.set_page_config(
    page_title="Cortex Search Demo",
    page_icon="🔍",
    layout="wide"
)

# Service configurations with boost/decay options
SEARCH_SERVICES = {
    "Company Transcripts": {
        "service": "SNOWFLAKE_PUBLIC_DATA_CORTEX_KNOWLEDGE_EXTENSIONS.AI.COMPANY_EVENT_TRANSCRIPT_CORTEX_SEARCH_SERVICE",
        "columns": ["CHUNK", "COMPANY", "EVENT_TYPE", "FISCAL_PERIOD", "EVENT_TIME"],
        "filter_columns": ["COMPANY", "EVENT_TYPE", "FISCAL_PERIOD"],
        "text_column": "CHUNK",
        "description": "Earnings calls & investor days (11M+ chunks)",
        "time_decay_columns": ["EVENT_TIME"],
        "numeric_boost_columns": [],
        "sample_questions": [
            "What did technology companies say about AI investments?",
            "How has Oracle discussed cloud computing strategy?",
            "What guidance did enterprise software companies give for fiscal 2025?",
            "What are the main concerns CFOs mentioned about margin pressure?",
        ]
    },
    "Snowflake Docs": {
        "service": "SNOWFLAKE_DOCUMENTATION.SHARED.CKE_SNOWFLAKE_DOCS_SERVICE",
        "columns": ["CHUNK", "DOCUMENT_TITLE", "SOURCE_URL"],
        "filter_columns": ["DOCUMENT_TITLE"],
        "text_column": "CHUNK",
        "description": "Official Snowflake documentation (45K+ chunks)",
        "time_decay_columns": [],
        "numeric_boost_columns": [],
        "sample_questions": [
            "How do I build a Streamlit app in Snowflake?",
            "What Cortex LLM functions are available?",
            "How do I set up a data pipeline with streams and tasks?",
            "Best practices for role-based access control in Snowflake",
        ]
    },
    "ISDA Documents": {
        "service": "ISDA_DOCUMENT_POC.SEMANTIC_VIEWS.ISDA_DOCUMENT_SEARCH",
        "columns": ["DOCUMENT_ID", "FILENAME", "PARTY_A", "PARTY_B", "FULL_TEXT"],
        "filter_columns": ["PARTY_A", "PARTY_B"],
        "text_column": "FULL_TEXT",
        "description": "ISDA master agreements (4 docs)",
        "time_decay_columns": [],
        "numeric_boost_columns": [],
        "sample_questions": [
            "What is the cross default threshold?",
            "Automatic early termination provisions",
            "Bankruptcy event triggers",
            "Credit support annex requirements",
        ]
    },
    "Research Notes": {
        "service": "FSI_DEMO_DB.QRI.RESEARCH_NOTES_SEARCH",
        "columns": ["NOTE_ID", "PUBLISH_DATE", "ANALYST_NAME", "TICKER", "COMPANY_NAME", "SECTOR", "NOTE_TYPE", "TITLE", "BODY", "RATING", "CONVICTION"],
        "filter_columns": ["ANALYST_NAME", "TICKER", "COMPANY_NAME", "SECTOR", "NOTE_TYPE", "RATING"],
        "text_column": "BODY",
        "description": "Analyst research notes (10 notes)",
        "time_decay_columns": ["PUBLISH_DATE"],
        "numeric_boost_columns": ["CONVICTION"],
        "sample_questions": [
            "What is the investment thesis for technology stocks?",
            "Which companies have a buy rating?",
            "What are analysts saying about market risks?",
            "Recent rating changes and analyst convictions",
        ]
    },
}


@st.cache_resource
def get_session():
    """Get Snowpark session for Streamlit in Snowflake."""
    return get_active_session()


@st.cache_data(ttl=60)
def get_filter_values(service_name: str, column: str) -> list[str]:
    """Get distinct values for a filter column by querying the search service."""
    session = get_session()
    service_config = SEARCH_SERVICES[service_name]
    service_path = service_config["service"]
    
    # Query the service to get sample values for the filter column
    request = {
        "query": "*",
        "columns": [column],
        "limit": 100
    }
    
    try:
        result = session.sql(f"""
            SELECT SNOWFLAKE.CORTEX.SEARCH_PREVIEW(
                '{service_path}',
                '{json.dumps(request)}'
            ) as RESULT
        """).to_pandas()
        
        result_json = json.loads(result.iloc[0]["RESULT"])
        
        # Extract unique values
        values = set()
        for r in result_json.get("results", []):
            val = r.get(column)
            if val:
                values.add(str(val) if not isinstance(val, str) else val)
        
        return sorted(list(values))[:50]  # Limit to 50 options
    except Exception:
        return []


def execute_cortex_search(
    service_name: str,
    query: str,
    filters: dict[str, str] | None = None,
    limit: int = 5,
    scoring_config: dict | None = None,
) -> tuple[dict, dict, float]:
    """
    Execute Cortex Search and return results with timing.
    
    Returns:
        tuple of (results_dict, request_dict, latency_seconds)
    """
    session = get_session()
    service_config = SEARCH_SERVICES[service_name]
    service_path = service_config["service"]
    columns = service_config["columns"]
    
    # Build filter clause
    filter_list = []
    if filters:
        for col, val in filters.items():
            if val and val != "All":
                filter_list.append({"@eq": {col: val}})
    
    filter_clause = None
    if len(filter_list) == 1:
        filter_clause = filter_list[0]
    elif len(filter_list) > 1:
        filter_clause = {"@and": filter_list}
    
    # Build request
    request = {
        "query": query,
        "columns": columns,
        "limit": limit,
        "response_groups": ["score", "search_component_scores"]
    }
    
    if filter_clause:
        request["filter"] = filter_clause
    
    if scoring_config:
        request["scoring_config"] = scoring_config
    
    # Execute with timing
    start_time = time.time()
    
    result = session.sql(f"""
        SELECT SNOWFLAKE.CORTEX.SEARCH_PREVIEW(
            '{service_path}',
            '{json.dumps(request)}'
        ) as RESULT
    """).to_pandas()
    
    latency = time.time() - start_time
    
    # Parse result
    result_json = json.loads(result.iloc[0]["RESULT"])
    
    return result_json, request, latency


def render_score_breakdown(scores: dict):
    """Render component scores as a visual breakdown."""
    col1, col2, col3 = st.columns(3)
    
    with col1:
        text_match = scores.get("text_match", 0)
        st.metric(
            "Text Match (BM25)",
            f"{text_match:.4f}",
            help="Lexical matching score based on term frequency"
        )
    
    with col2:
        cosine = scores.get("cosine_similarity", 0)
        st.metric(
            "Semantic Similarity",
            f"{cosine:.4f}",
            help="Cosine similarity from vector embeddings"
        )
    
    with col3:
        reranker = scores.get("reranker_score", 0)
        st.metric(
            "Reranker Score",
            f"{reranker:.4f}",
            help="Cross-encoder reranking score for final ordering"
        )


def calculate_dcg(relevances: list[int], k: int) -> float:
    """Calculate Discounted Cumulative Gain at position k."""
    dcg = 0.0
    for i, rel in enumerate(relevances[:k]):
        dcg += rel / math.log2(i + 2)  # +2 because i is 0-indexed and log2(1) = 0
    return dcg


def calculate_ndcg(relevances: list[int], ideal_relevances: list[int], k: int) -> float:
    """Calculate Normalized DCG at position k."""
    dcg = calculate_dcg(relevances, k)
    idcg = calculate_dcg(sorted(ideal_relevances, reverse=True), k)
    return dcg / idcg if idcg > 0 else 0.0


def calculate_precision_at_k(relevant_count: int, k: int) -> float:
    """Calculate Precision@K (relevant items in top k / k)."""
    return relevant_count / k if k > 0 else 0.0


def calculate_recall_at_k(relevant_count: int, total_relevant: int) -> float:
    """Calculate Recall@K (relevant items found / total relevant)."""
    return relevant_count / total_relevant if total_relevant > 0 else 0.0


@st.cache_data(ttl=300)
def load_golden_dataset() -> list[dict]:
    """Load the golden evaluation dataset."""
    session = get_session()
    try:
        df = session.sql("""
            SELECT QUERY, DOCUMENT, RELEVANCY_SCORE 
            FROM FSI_DEMO_DB.QRI.RESEARCH_NOTES_EVAL_GOLDEN
        """).to_pandas()
        return df.to_dict('records')
    except Exception:
        return []


def run_evaluation(k_values: list[int] = [3, 5, 10]) -> dict:
    """
    Run evaluation against the Research Notes search service.
    Returns metrics for each query and aggregate metrics.
    """
    session = get_session()
    golden_data = load_golden_dataset()
    
    if not golden_data:
        return {"error": "No golden dataset found"}
    
    # Group by query to get all relevant documents per query
    queries = {}
    for row in golden_data:
        query = row["QUERY"]
        if query not in queries:
            queries[query] = []
        queries[query].append({
            "document": row["DOCUMENT"],
            "relevancy": row["RELEVANCY_SCORE"]
        })
    
    results = {
        "queries": [],
        "aggregate": {k: {"ndcg": [], "precision": [], "recall": []} for k in k_values}
    }
    
    service_config = SEARCH_SERVICES["Research Notes"]
    
    for query_text, expected_docs in queries.items():
        # Execute search
        try:
            search_results, _, latency = execute_cortex_search(
                service_name="Research Notes",
                query=query_text,
                limit=max(k_values)
            )
        except Exception as e:
            continue
        
        # Get returned document texts
        returned_docs = []
        for r in search_results.get("results", []):
            doc_text = r.get(service_config["text_column"], "")
            returned_docs.append(doc_text)
        
        # Calculate relevance scores for returned results
        relevances = []
        for doc in returned_docs:
            # Find matching expected doc using exact equality
            best_rel = 0
            for exp in expected_docs:
                # Exact match on full document text
                if doc.strip() == exp["document"].strip():
                    best_rel = max(best_rel, exp["relevancy"])
            relevances.append(best_rel)
        
        # Ideal relevances (sorted desc)
        ideal_relevances = sorted([e["relevancy"] for e in expected_docs], reverse=True)
        
        # Calculate metrics for each k
        query_metrics = {"query": query_text, "latency_ms": latency * 1000, "metrics": {}}
        
        for k in k_values:
            ndcg = calculate_ndcg(relevances, ideal_relevances, k)
            
            # Count relevant items (relevancy >= 2) in top k
            relevant_in_k = sum(1 for r in relevances[:k] if r >= 2)
            total_relevant = sum(1 for e in expected_docs if e["relevancy"] >= 2)
            
            precision = calculate_precision_at_k(relevant_in_k, min(k, len(relevances)))
            recall = calculate_recall_at_k(relevant_in_k, total_relevant)
            
            query_metrics["metrics"][k] = {
                "ndcg": ndcg,
                "precision": precision,
                "recall": recall
            }
            
            results["aggregate"][k]["ndcg"].append(ndcg)
            results["aggregate"][k]["precision"].append(precision)
            results["aggregate"][k]["recall"].append(recall)
        
        results["queries"].append(query_metrics)
    
    # Calculate aggregate means
    for k in k_values:
        for metric in ["ndcg", "precision", "recall"]:
            values = results["aggregate"][k][metric]
            results["aggregate"][k][f"{metric}_mean"] = sum(values) / len(values) if values else 0
    
    return results


def render_result_card(result: dict, index: int, service_config: dict):
    """Render a single search result as an expandable card."""
    scores = result.get("@scores", {})
    text_column = service_config["text_column"]
    columns = service_config["columns"]
    
    # Get title - use first non-text column as title
    title_col = [c for c in columns if c != text_column][0] if columns else "Result"
    title = result.get(title_col, f"Result {index + 1}")
    if isinstance(title, str) and len(title) > 60:
        title = title[:60] + "..."
    
    with st.expander(f"**#{index + 1}** {title}", expanded=(index == 0)):
        # Show all metadata columns
        metadata_cols = [c for c in columns if c != text_column]
        if metadata_cols:
            # Display metadata in rows of 3 columns
            for i in range(0, len(metadata_cols), 3):
                row_cols = metadata_cols[i:i+3]
                meta_str = " | ".join([f"**{c}:** {result.get(c, 'N/A')}" for c in row_cols])
                st.markdown(meta_str)
        
        st.divider()
        
        # Score breakdown
        st.markdown("##### Component Scores")
        render_score_breakdown(scores)
        
        # Show text content
        text_content = result.get(text_column, "")
        if text_content:
            st.divider()
            st.markdown("##### Content Preview")
            preview = text_content[:1500] + "..." if len(text_content) > 1500 else text_content
            st.code(preview, language=None)


# Main UI
st.title("Cortex Search Demo")
st.markdown("*Explore multiple Cortex Search services with advanced features*")

# Sidebar for configuration (always visible)
with st.sidebar:
    st.header("Search Configuration")
    
    # Service selector
    st.subheader("Select Service")
    selected_service = st.radio(
        "Choose a Cortex Search service:",
        options=list(SEARCH_SERVICES.keys()),
        format_func=lambda x: f"{x}",
        label_visibility="collapsed"
    )
    
    service_config = SEARCH_SERVICES[selected_service]
    st.caption(service_config["description"])
    
    st.divider()
    
    # Dynamic filters
    st.subheader("Filters")
    
    active_filters = {}
    filter_columns = service_config["filter_columns"]
    
    if filter_columns:
        # Let user choose which column to filter on
        filter_column = st.selectbox(
            "Filter by attribute:",
            options=["None"] + filter_columns
        )
        
        if filter_column != "None":
            # Get values for selected column
            with st.spinner("Loading values..."):
                filter_values = get_filter_values(selected_service, filter_column)
            
            if filter_values:
                selected_value = st.selectbox(
                    f"Select {filter_column}:",
                    options=["All"] + filter_values
                )
                if selected_value != "All":
                    active_filters[filter_column] = selected_value
            else:
                st.caption("No filter values available")
    else:
        st.caption("No filter columns for this service")
    
    st.divider()
    
    # Scoring Configuration
    st.subheader("Scoring Config")
    
    scoring_config = None
    scoring_functions = {}
    
    # Time Decay
    time_decay_cols = service_config.get("time_decay_columns", [])
    if time_decay_cols:
        enable_time_decay = st.checkbox(
            "Enable Time Decay (boost recent)",
            help="Boost more recent documents based on timestamp"
        )
        if enable_time_decay:
            decay_col = st.selectbox("Time column:", time_decay_cols)
            decay_weight = st.slider("Decay weight:", 0.5, 5.0, 1.0, 0.5)
            decay_hours = st.selectbox(
                "Decay window:",
                options=[168, 720, 2160, 8760],
                format_func=lambda x: {168: "1 week", 720: "1 month", 2160: "3 months", 8760: "1 year"}[x]
            )
            scoring_functions["time_decays"] = [
                {"column": decay_col, "weight": decay_weight, "limit_hours": decay_hours}
            ]
    else:
        st.caption("No time columns for decay")
    
    # Numeric Boost
    numeric_boost_cols = service_config.get("numeric_boost_columns", [])
    if numeric_boost_cols:
        enable_numeric_boost = st.checkbox(
            "Enable Numeric Boost",
            help="Boost results by numeric metadata (e.g., popularity, conviction)"
        )
        if enable_numeric_boost:
            boost_col = st.selectbox("Boost column:", numeric_boost_cols)
            boost_weight = st.slider("Boost weight:", 0.5, 5.0, 2.0, 0.5)
            scoring_functions["numeric_boosts"] = [
                {"column": boost_col, "weight": boost_weight}
            ]
    else:
        st.caption("No numeric columns for boosting")
    
    # Disable Reranker option
    disable_reranker = st.checkbox(
        "Disable Reranker",
        help="Skip cross-encoder reranking step (faster but may reduce quality)"
    )
    
    # Build scoring config if any options selected
    if scoring_functions or disable_reranker:
        scoring_config = {}
        if scoring_functions:
            scoring_config["functions"] = scoring_functions
        if disable_reranker:
            scoring_config["disable_reranker"] = True
    
    st.divider()
    
    # Options
    st.subheader("Options")
    
    num_results = st.slider(
        "Number of Results",
        min_value=1,
        max_value=10,
        value=5
    )
    
    st.divider()
    
    st.markdown("""
    ### About This Demo
    
    **Cortex Search** capabilities:
    
    - **Multi-Service**: Different search indexes
    - **Hybrid Search**: BM25 + semantic + reranker
    - **Filtering**: Attribute-based filtering
    - **Scoring Config**: Time decay & numeric boost
    - **Component Scores**: Detailed scoring signals
    - **Evaluation**: NDCG, Precision, Recall metrics
    """)

# Create tabs for Search and Evaluation
search_tab, eval_tab = st.tabs(["Search", "Evaluation"])

# ============== SEARCH TAB ==============
with search_tab:
    st.markdown(f"### Search: {selected_service}")
    
    # Sample questions
    st.markdown("**Try a sample question:**")
    sample_cols = st.columns(2)
    sample_questions = service_config["sample_questions"]
    
    # Use session state to track selected sample
    if "sample_query" not in st.session_state:
        st.session_state.sample_query = ""
    
    for i, q in enumerate(sample_questions):
        col = sample_cols[i % 2]
        with col:
            if st.button(q, key=f"sample_{i}", use_container_width=True):
                st.session_state.sample_query = q
    
    # Search input
    query = st.text_input(
        "Enter your search query",
        value=st.session_state.sample_query,
        placeholder="Type your question or click a sample above...",
        label_visibility="collapsed"
    )
    
    # Clear sample query after use
    if query != st.session_state.sample_query:
        st.session_state.sample_query = ""
    
    # Execute search
    if query:
        st.divider()
        
        # Show active configuration
        config_items = []
        if active_filters:
            filter_str = " AND ".join([f"{k} = {v}" for k, v in active_filters.items()])
            config_items.append(f"**Filters:** {filter_str}")
        if scoring_config:
            if "functions" in scoring_config:
                if "time_decays" in scoring_config["functions"]:
                    td = scoring_config["functions"]["time_decays"][0]
                    config_items.append(f"**Time Decay:** {td['column']} (weight={td['weight']})")
                if "numeric_boosts" in scoring_config["functions"]:
                    nb = scoring_config["functions"]["numeric_boosts"][0]
                    config_items.append(f"**Numeric Boost:** {nb['column']} (weight={nb['weight']})")
            if scoring_config.get("disable_reranker"):
                config_items.append("**Reranker:** Disabled")
        
        if config_items:
            st.info(" | ".join(config_items))
        
        # Execute search
        with st.spinner("Searching..."):
            try:
                results, request, latency = execute_cortex_search(
                    service_name=selected_service,
                    query=query,
                    filters=active_filters if active_filters else None,
                    limit=num_results,
                    scoring_config=scoring_config,
                )
                
                # Results header with metrics
                result_count = len(results.get("results", []))
                request_id = results.get("request_id", "")
                
                col1, col2 = st.columns([3, 1])
                with col1:
                    st.markdown(f"### Results ({result_count} documents)")
                with col2:
                    st.metric("Query Latency", f"{latency*1000:.0f} ms")
                
                st.caption(f"**Request ID:** `{request_id}`")
                
                # Display results
                if result_count > 0:
                    for idx, result in enumerate(results["results"]):
                        render_result_card(result, idx, service_config)
                else:
                    st.warning("No results found. Try broadening your search or removing filters.")
                
                # Raw request and response (collapsible)
                with st.expander("View Raw API Request"):
                    st.markdown(f"**Service:** `{service_config['service']}`")
                    st.json(request)
                
                with st.expander("View Raw API Response"):
                    st.json(results)
                    
            except Exception as e:
                st.error(f"Search failed: {str(e)}")
    
    else:
        # Empty state
        st.info("Enter a search query above or click a sample question to get started.")
        
        # Feature highlights
        st.markdown("### Features Demonstrated")
        
        feat_cols = st.columns(4)
        
        with feat_cols[0]:
            st.markdown("""
            **Multi-Service**
            
            Toggle between ISDA docs,
            company transcripts, and
            Snowflake documentation.
            """)
        
        with feat_cols[1]:
            st.markdown("""
            **Dynamic Filters**
            
            Filter by any metadata
            attribute with live
            value population.
            """)
        
        with feat_cols[2]:
            st.markdown("""
            **Scoring Config**
            
            Time decay for recency,
            numeric boost for popularity,
            disable reranker option.
            """)
        
        with feat_cols[3]:
            st.markdown("""
            **Component Scores**
            
            See text_match, cosine_similarity,
            and reranker_score for
            each result.
            """)

# ============== EVALUATION TAB ==============
with eval_tab:
    st.markdown("### Research Notes Search Evaluation")
    st.markdown("""
    Evaluate search quality using a golden dataset of queries with known relevant documents.
    Metrics calculated: **NDCG**, **Precision@K**, and **Recall@K**.
    """)
    
    # Show golden dataset info
    golden_data = load_golden_dataset()
    
    if golden_data:
        st.success(f"Golden dataset loaded: **{len(golden_data)}** query-document pairs")
        
        # Show golden dataset preview
        with st.expander("View Golden Dataset"):
            import pandas as pd
            df = pd.DataFrame(golden_data)
            st.dataframe(df, use_container_width=True)
        
        st.divider()
        
        # Run evaluation button
        if st.button("Run Evaluation", type="primary", use_container_width=True):
            with st.spinner("Running evaluation across all queries..."):
                eval_results = run_evaluation(k_values=[3, 5, 10])
            
            if "error" in eval_results:
                st.error(eval_results["error"])
            else:
                # Display aggregate metrics
                st.markdown("### Aggregate Metrics")
                
                metric_cols = st.columns(3)
                k_values = [3, 5, 10]
                
                for i, k in enumerate(k_values):
                    with metric_cols[i]:
                        st.markdown(f"#### @K={k}")
                        agg = eval_results["aggregate"][k]
                        st.metric("NDCG", f"{agg['ndcg_mean']:.3f}")
                        st.metric("Precision", f"{agg['precision_mean']:.3f}")
                        st.metric("Recall", f"{agg['recall_mean']:.3f}")
                
                st.divider()
                
                # Per-query breakdown
                st.markdown("### Per-Query Results")
                
                for qr in eval_results["queries"]:
                    with st.expander(f"**{qr['query'][:60]}...** ({qr['latency_ms']:.0f}ms)"):
                        pcols = st.columns(3)
                        for i, k in enumerate(k_values):
                            with pcols[i]:
                                st.markdown(f"**@K={k}**")
                                m = qr["metrics"][k]
                                st.write(f"NDCG: {m['ndcg']:.3f}")
                                st.write(f"Precision: {m['precision']:.3f}")
                                st.write(f"Recall: {m['recall']:.3f}")
                
                # Raw results
                with st.expander("View Raw Evaluation Results"):
                    st.json(eval_results)
    else:
        st.warning("No golden dataset found. Create the evaluation dataset first.")
        st.code("""
-- Create golden dataset table
CREATE TABLE FSI_DEMO_DB.QRI.RESEARCH_NOTES_EVAL_GOLDEN (
    QUERY VARCHAR,
    DOCUMENT VARCHAR,
    RELEVANCY_SCORE INTEGER  -- 0=Irrelevant, 1=Slightly, 2=Somewhat, 3=Perfectly
);
        """, language="sql")
