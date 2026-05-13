# Sample Data

## About

This demo requires equity research PDF reports uploaded to the `@REPORTS` stage. PDFs are not committed to this repository due to size and licensing constraints.

## Sample Reports Used in Development

The following reports were used when building this demo:

| File | Source |
|------|--------|
| `Global Economics Analyst_ The Path to 2075 — The Positive Story of Global Aging (Daly_Njie_Allen).pdf` | Goldman Sachs |
| `US Stocks Are Forecast to Rise 6% in 2026 _ Goldman Sachs.pdf` | Goldman Sachs |

## Sourcing Your Own Reports

You can use any equity research PDFs. Good sources include:

- Your firm's internal research library
- Publicly available research from broker websites
- Academic working papers on financial topics
- Annual reports and investor presentations (PDF format)

## Upload Instructions

After placing PDFs in this directory locally, upload them to the stage:

```bash
# Using Snowflake CLI
for f in *.pdf; do
    snow stage copy "$f" @FSI_DEMO_DB.EQUITY_RESEARCH.REPORTS --overwrite
done
```

Or use the Snowsight UI to drag and drop files directly onto the stage.

After uploading, refresh the stage directory:

```sql
ALTER STAGE FSI_DEMO_DB.EQUITY_RESEARCH.REPORTS REFRESH;
```
