# hotel-wbr

A [Claude Code](https://claude.ai/code) skill that generates the **Hotels KPI (scorecard)** section of Skyscanner's Weekly Business Review (WBR).

## What it does

Given a reporting week, the skill:

1. Queries Databricks (`prod_trusted_gold`) for topline metrics — declared revenue, generated revenue, sessions, redirects, and eCPC — with both WoW and YoY comparisons
2. Breaks down sessions and redirects/session by traffic source
3. Checks bidding activity (`auction_insights_agg`) to attribute eCPC changes to bid changes (CPC markets) vs. H2/booking value (CPA markets)
4. Writes the narrative callouts in the standard WBR style
5. Produces a self-contained HTML report with 4 embedded charts:
   - **Chart 1** — Session WoW contribution by traffic source (horizontal waterfall, K labels + WoW%)
   - **Chart 2** — R/S dual panel: absolute value (this vs prev week) + WoW% change by traffic source
   - **Chart 3** — Partner eCPC daily trend
   - **Chart 4** — Partner eCPC WoW% bar chart

## Usage

Install the skill in Claude Code, then ask:

```
Generate the Hotels WBR for the week of 27 Mar – 2 Apr 2026.
```

or just:

```
Write up this week's hotels WBR.
```

Claude will determine the correct reporting period, run the queries, write the callouts, and save an HTML file ready to open in a browser or upload to Confluence.

## Structure

```
hotel-wbr/
  SKILL.md          # skill definition and step-by-step instructions
  evals/
    evals.json      # evaluation prompts and assertions
```

## Key data sources

| Table | Used for |
|---|---|
| `prod_trusted_gold.revenue_analytics.w_daily_kpi_agg` | Declared revenue, redirects, sessions (WoW + YoY) |
| `prod_trusted_gold.hotel_core_business_metrics.w_hotel_summary_aggregated` | Generated revenue and per-partner eCPC |
| `prod_trusted_gold.session.v_hotel_session_metrics` | Session + redirect breakdown by traffic source |
| `prod_trusted_gold.hotel_commercial_partner_analytics.auction_insights_agg` | Bid-level data for CPC markets (eCPC attribution) |
| `prod_trusted_gold.revenue_analytics.revenue_referral_event` | HPA vs PPA revenue split (optional deep-dive) |
