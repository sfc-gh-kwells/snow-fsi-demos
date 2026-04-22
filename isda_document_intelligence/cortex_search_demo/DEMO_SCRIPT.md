# Cortex Search Demo Script
## ISDA Document Intelligence

**Duration:** 5-7 minutes  
**Audience:** Customers interested in enterprise search, RAG, document intelligence  
**App URL:** `https://app.snowflake.com/<YOUR_ORG>/<YOUR_ACCOUNT>/#/streamlit-apps/ISDA_DOCUMENT_POC.SEMANTIC_VIEWS.CORTEX_SEARCH_DEMO`

---

## Setup Checklist

- [ ] Open the Streamlit app in Snowsight
- [ ] Clear any previous searches
- [ ] Set filters to "All" 
- [ ] Have the sidebar visible

---

## Demo Flow

### 1. Introduction (30 seconds)

> "Let me show you Cortex Search - Snowflake's native enterprise search service. This demo uses real ISDA Master Agreements - complex legal documents that banks use for derivatives trading. We've indexed 9 documents and I'll show you how Cortex Search finds relevant information instantly."

**Key points to mention:**
- No external search infrastructure needed
- Data never leaves Snowflake
- Automatic embedding generation and indexing

---

### 2. Basic Search - Show Hybrid Search (1 minute)

**Action:** Type `cross default threshold` and press Enter

> "Let's search for 'cross default threshold' - this is a key provision in ISDA agreements that triggers default if a party defaults on other obligations."

**Point out:**
- Results appear in ~200-400ms (highlight the latency metric)
- Multiple documents returned, ranked by relevance

> "Notice the latency - under half a second. This is a hybrid search combining traditional keyword matching with semantic understanding."

---

### 3. Component Scores - Explain the Ranking (1.5 minutes)

**Action:** Expand the first result to show the score breakdown

> "Here's what makes Cortex Search powerful - let me show you the three scoring components:"

**Walk through each score:**

| Score | Explanation |
|-------|-------------|
| **Text Match (BM25)** | "This is traditional lexical search - how well do the exact words match? Higher means more keyword overlap." |
| **Semantic Similarity** | "This is the embedding-based score using Arctic Embed. It understands meaning - so 'default threshold' matches even if the document says 'threshold amount for default events'." |
| **Reranker Score** | "This is a cross-encoder that looks at the query and document together for final ranking. It's the most computationally expensive but most accurate signal." |

> "Cortex Search automatically combines all three signals to give you the best results. You don't have to tune anything."

**Action:** Click the "How are scores calculated?" popover for additional detail

---

### 4. Filtering by Attributes (1 minute)

**Action:** Select "BARCLAYS BANK PLC" from the Party A dropdown

> "Now let's say I only want to search within Barclays agreements. Cortex Search supports attribute filtering - I can filter by any metadata column."

**Point out:**
- The filter badge appears showing "Party A = BARCLAYS BANK PLC"
- Results are now scoped to only Barclays documents
- Latency is still fast because filtering happens at the index level

> "This is pre-filtering, not post-filtering. The search service only looks at matching documents, so it stays fast even with millions of records."

**Action:** Add Party B filter to show AND logic

> "I can combine filters too - this uses AND logic. Now I'm searching only the Barclays/World Omni agreement."

---

### 5. Different Query - Semantic Understanding (1 minute)

**Action:** Clear filters, search for `what happens if a party goes bankrupt`

> "Let me show you the semantic understanding. I'll search for 'what happens if a party goes bankrupt' - natural language, not keywords."

**Point out:**
- Results include documents about "Bankruptcy" events even though the exact phrase isn't in the documents
- The semantic similarity score is high because the model understands the meaning

> "The documents talk about 'Events of Default' and 'Bankruptcy' provisions - Cortex Search understands that bankruptcy and 'party goes bankrupt' mean the same thing."

---

### 6. Document Preview - Citations (45 seconds)

**Action:** Check "Include Document Text" in sidebar, run another search

> "For RAG applications, you need to see what text matched. Let me enable document preview."

**Action:** Expand a result to show the FULL_TEXT

> "Now I can see the actual document content. In a RAG application, this is what you'd send to the LLM as context. The search service handles the retrieval, you handle the generation."

---

### 7. Wrap-Up - Architecture (30 seconds)

> "Let me recap what's happening under the hood:"

**Key architecture points:**
1. Documents are stored in Snowflake tables
2. Cortex Search automatically generates embeddings using Arctic Embed
3. The service maintains a hybrid index (BM25 + vector)
4. Queries go through embedding → retrieval → reranking
5. All computation stays in Snowflake - no external APIs

> "This is fully managed. You define the search service with a SQL statement, and Snowflake handles indexing, embedding updates, and query serving automatically."

---

## Handling Questions

### "How does this compare to [Elasticsearch/Pinecone/etc]?"

> "Key differences: 
> 1. Your data never leaves Snowflake - no sync jobs or data movement
> 2. It's a hybrid search by default - you get BM25 + semantic + reranking out of the box
> 3. It's fully managed - no cluster sizing, no index maintenance
> 4. It uses the same governance and access control as your other Snowflake data"

### "What's the latency at scale?"

> "We're seeing sub-second latency on production workloads with millions of documents. The service is optimized for real-time retrieval use cases."

### "How do I use this with an LLM?"

> "You call the search service to get relevant chunks, then pass those to CORTEX.COMPLETE() or any LLM. We also have Cortex Agents that combine search + LLM automatically, but this demo shows the search component in isolation."

### "What about chunking?"

> "Cortex Search works with whatever text you give it. For long documents, you'd typically chunk them before indexing. We also have PARSE_DOCUMENT to extract text from PDFs and other formats."

### "Can I filter on numeric ranges or dates?"

> "Yes - Cortex Search supports @eq, @gt, @lt, @gte, @lte, @in, and @and/@or for combining filters. You can filter on any attribute column defined in the service."

---

## Sample Queries to Have Ready

| Query | Shows |
|-------|-------|
| `cross default threshold` | Basic hybrid search |
| `what happens if a party goes bankrupt` | Semantic understanding |
| `automatic early termination` | Legal concept search |
| `close-out netting` | Financial term search |
| `governing law New York` | Multi-term query |

---

## Technical Details (If Asked)

**Search Service Definition:**
```sql
CREATE CORTEX SEARCH SERVICE ISDA_DOCUMENT_SEARCH
  ON FULL_TEXT
  ATTRIBUTES DOCUMENT_ID, FILENAME, PARTY_A, PARTY_B
  WAREHOUSE = COMPUTE_WH
  TARGET_LAG = '1 hour'
  AS (SELECT ... FROM documents);
```

**API Call (Python):**
```python
result = session.sql("""
    SELECT SNOWFLAKE.CORTEX.SEARCH_PREVIEW(
        'ISDA_DOCUMENT_POC.SEMANTIC_VIEWS.ISDA_DOCUMENT_SEARCH',
        '{"query": "...", "columns": [...], "limit": 5,
          "filter": {"@eq": {"PARTY_A": "BARCLAYS"}},
          "response_groups": ["score", "search_component_scores"]}'
    )
""").collect()
```

**Embedding Model:** snowflake-arctic-embed-m-v1.5 (automatic)

**Index Refresh:** Incremental, with 1-hour target lag (configurable)
