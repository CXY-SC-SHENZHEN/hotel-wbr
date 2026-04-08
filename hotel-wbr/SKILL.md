---
name: hotel-wbr
description: >
  Generates the Hotels KPI (scorecard) section of the Skyscanner WBR (Weekly Business Review) report.
  Use this skill whenever the user asks to generate, write, draft, or prepare a hotel WBR, hotel weekly business review,
  hotel weekly performance report, hotel KPI scorecard, or any hotel weekly callout. Also use it when the user says
  things like "write up this week's hotels" or "prepare the hotels section for WBR" or "what happened with hotels this week".
  The skill queries Databricks tables to compute the actual numbers, then writes the report in the standard WBR format.
---

# Hotel WBR Report Generator

## What this skill does

Generates the **Hotels KPI (scorecard)** section for Skyscanner's WBR. The output should be ready to paste into Confluence.

## Report Period

The WBR covers the **7-day period ending on the previous Thursday** (Fri–Thu). When the user asks for the report without specifying a date:
- Find the most recent Thursday before today
- The week covers the 7 days ending on that Thursday: `[Thursday - 6 days]` to `[Thursday]`
- e.g. if today is Monday April 7 2026, the previous Thursday is April 2 2026, so the report covers March 27 – April 2 2026

If the user specifies a date range, use that instead.

## Output Format

The report must follow this exact structure, matching the WBR Confluence page format:

```
**Hotels Weekly Performance Callouts**
_[start date] - [end date]_

**HIGHLIGHT OF THE WEEK:**

[One paragraph summary: state whether declared revenue went up/down and by what %, and generated revenue up/down by what %. Then explain the primary driver using the revenue equation: Sessions × Redirects/Session × Revenue/Redirect. State each component's WoW movement. Keep it to 2-3 sentences.]

**Overview of Hotel Metrics WoW**

[RAG_EMOJI] **Sessions [WoW%] WoW [YoY%] YoY**

* [Traffic source breakdown: which sources drove the movement, noting % changes for the major ones]
* [Market or device-level callouts if there's a significant driver]

[RAG_EMOJI] **Redirects/Session [WoW%] WoW [YoY%] YoY**

* [Key callouts: which traffic sources saw conversion changes, any notable partner or market drivers]

[RAG_EMOJI] **Revenue/Redirect [WoW%] WoW [YoY%] YoY**

* With generated revenue, revenue/redirect went [up/down] by [X]%
* [Partner eCPC movements: name the key partners (Trip, Agoda, Vio, Booking, etc.) and their WoW eCPC changes]
```

**Topline table must include YoY for all 7 metrics** (never leave a YoY cell blank):
- Declared Revenue YoY: `declared_rev_yoy` from Step 2
- Generated Revenue YoY: `gen_rev_yoy` from Step 3
- Sessions YoY: `sessions_yoy` from Step 2
- Redirects YoY: `redirects_yoy` from Step 2
- Redirects/Session YoY: derived as `(redirects_yoy/sessions_yoy)` vs this week
- Declared eCPC YoY: `declared_rev_yoy / redirects_yoy` vs this week
- Generated eCPC YoY: `gen_rev_yoy / redirects_yoy` vs this week

**RAG status rules:**
- 🟢 green_circle: metric is up WoW, or a small decline (<3%) with no significant concern
- 🟠 orange_circle: metric is down moderately (-3% to -10% WoW), or has a notable anomaly worth watching
- 🔴 red_circle: metric is down significantly (>10% WoW), or has a serious issue

## Step-by-Step Approach

### Step 1: Determine the report date range

Identify `week_start` (Monday) and `week_end` (Sunday/Thursday) for the current and prior week, and the same period last year (YoY).

### Step 2: Query topline metrics

Use `prod_trusted_gold.revenue_analytics.w_daily_kpi_agg` for the headline numbers.

**Important:** Query Sessions without the `RevenueSourceCategory` filter (it lives across all rows), but query Revenue and Redirects with `RevenueSourceCategory = 'Hotels'`. Run YoY as a separate query to stay within the 30-day date range limit.

**This week + previous week (WoW):**
```sql
SELECT
  SUM(CASE WHEN Date BETWEEN '[week_start]' AND '[week_end]' THEN Revenue ELSE 0 END) AS declared_rev_this,
  SUM(CASE WHEN Date BETWEEN '[prev_week_start]' AND '[prev_week_end]' THEN Revenue ELSE 0 END) AS declared_rev_prev,
  SUM(CASE WHEN Date BETWEEN '[week_start]' AND '[week_end]' THEN Redirects_Count ELSE 0 END) AS redirects_this,
  SUM(CASE WHEN Date BETWEEN '[prev_week_start]' AND '[prev_week_end]' THEN Redirects_Count ELSE 0 END) AS redirects_prev
FROM prod_trusted_gold.revenue_analytics.w_daily_kpi_agg
WHERE RevenueSourceCategory = 'Hotels'
  AND Date BETWEEN '[prev_week_start]' AND '[week_end]'
```

**Sessions (no category filter):**
```sql
SELECT
  SUM(CASE WHEN Date BETWEEN '[week_start]' AND '[week_end]' THEN Sessions_Hotels ELSE 0 END) AS sessions_this,
  SUM(CASE WHEN Date BETWEEN '[prev_week_start]' AND '[prev_week_end]' THEN Sessions_Hotels ELSE 0 END) AS sessions_prev
FROM prod_trusted_gold.revenue_analytics.w_daily_kpi_agg
WHERE Date BETWEEN '[prev_week_start]' AND '[week_end]'
```

**YoY — declared revenue + redirects (with Hotels filter):**
```sql
SELECT
  SUM(Revenue) AS declared_rev_yoy,
  SUM(Redirects_Count) AS redirects_yoy
FROM prod_trusted_gold.revenue_analytics.w_daily_kpi_agg
WHERE RevenueSourceCategory = 'Hotels'
  AND Date BETWEEN '[yoy_start]' AND '[yoy_end]'
```

**YoY — sessions (no category filter):**
```sql
SELECT SUM(Sessions_Hotels) AS sessions_yoy
FROM prod_trusted_gold.revenue_analytics.w_daily_kpi_agg
WHERE Date BETWEEN '[yoy_start]' AND '[yoy_end]'
```

From these results, compute all metrics and their YoY:
- **Sessions WoW%** = (sessions_this − sessions_prev) / sessions_prev; **YoY%** = (sessions_this − sessions_yoy) / sessions_yoy
- **Redirects WoW%** and **YoY%** directly from query results
- **R/S WoW%** = ((redirects_this/sessions_this) − (redirects_prev/sessions_prev)) / (redirects_prev/sessions_prev); **R/S YoY%** = same formula using yoy values
- **Declared eCPC WoW%** and **YoY%** = declared_rev / redirects for each period
- **Generated eCPC WoW%** and **YoY%** = gen_rev_total / redirects_this (see Step 3 for gen_rev YoY)

### Step 3: Get Generated Revenue and eCPC by partner

Use the following SQL to get generated revenue and per-partner eCPC:

```sql
SELECT
  case when website_id in ('h_hc') then 'h_h1' else website_id end as partner_id,
  SUM(CASE WHEN dt BETWEEN '[week_start]' AND '[week_end]' THEN estimated_revenue_gbp ELSE 0 END) AS gen_rev_this_week,
  SUM(CASE WHEN dt BETWEEN '[prev_week_start]' AND '[prev_week_end]' THEN estimated_revenue_gbp ELSE 0 END) AS gen_rev_prev_week,
  SUM(CASE WHEN dt BETWEEN '[week_start]' AND '[week_end]' THEN redirect_count ELSE 0 END) AS redirects_this_week,
  SUM(CASE WHEN dt BETWEEN '[prev_week_start]' AND '[prev_week_end]' THEN redirect_count ELSE 0 END) AS redirects_prev_week
FROM prod_trusted_gold.hotel_core_business_metrics.w_hotel_summary_aggregated
WHERE dt >= '[prev_week_start]'
  AND estimated_revenue_gbp <= 100000
GROUP BY 1
ORDER BY gen_rev_this_week DESC
LIMIT 20
```

Also run a **YoY query** for total generated revenue (to compute generated eCPC YoY):
```sql
SELECT SUM(estimated_revenue_gbp) AS gen_rev_yoy
FROM prod_trusted_gold.hotel_core_business_metrics.w_hotel_summary_aggregated
WHERE dt BETWEEN '[yoy_start]' AND '[yoy_end]'
  AND estimated_revenue_gbp <= 100000
```

Compute per-partner eCPC (gen_rev / redirects) WoW change. Key partner IDs: Booking.com = `h_bc`, Agoda = `h_ad`, Trip = `h_ct`, Expedia = `h_xp`, Hotels.com = `h_h1` (also `h_hc`, treat as same). Highlight partners where eCPC moved >5% WoW.

**Generated eCPC YoY%** = (gen_rev_this_total / redirects_this) / (gen_rev_yoy / redirects_yoy) − 1. The `gen_rev_this_total` is the sum across all partners from the WoW query.

### Step 4: Get session breakdown by traffic source

Use `prod_trusted_gold.session.v_hotel_session_metrics` — this table has a `traffic_source` field with human-readable labels that match the WBR standard format (e.g. `parallel_search`, `Paid HPA`, `Non-Brand SEM`, `Trip Advisor`, `Organic`, `Affiliate`, `direct_homepage`, `internal_other`, `Free HPA`, `Direct non-homepage`).

```sql
SELECT
  traffic_source,
  SUM(CASE WHEN dt BETWEEN '[week_start]' AND '[week_end]' THEN session_count ELSE 0 END) AS sessions_this,
  SUM(CASE WHEN dt BETWEEN '[prev_week_start]' AND '[prev_week_end]' THEN session_count ELSE 0 END) AS sessions_prev,
  SUM(CASE WHEN dt BETWEEN '[week_start]' AND '[week_end]' THEN redirect_count ELSE 0 END) AS redirects_this,
  SUM(CASE WHEN dt BETWEEN '[prev_week_start]' AND '[prev_week_end]' THEN redirect_count ELSE 0 END) AS redirects_prev
FROM prod_trusted_gold.session.v_hotel_session_metrics
WHERE dt BETWEEN '[prev_week_start]' AND '[week_end]'
GROUP BY 1
ORDER BY sessions_this DESC
```

Use `traffic_source` values directly in the WBR callouts — these are the standard names. Focus callouts on sources with the largest absolute or relative WoW change. For redirect/session, compute `redirect_count/session_count` per source and compare WoW.

### Step 5: Drill into market-level drivers (optional but recommended)

If sessions or redirect/session moved significantly, query by country to find the market driving it:

```sql
SELECT
  traveller_preferred_country_name,
  traffic_source,
  SUM(CASE WHEN dt BETWEEN '[week_start]' AND '[week_end]' THEN session_count ELSE 0 END) AS sessions_this,
  SUM(CASE WHEN dt BETWEEN '[prev_week_start]' AND '[prev_week_end]' THEN session_count ELSE 0 END) AS sessions_prev
FROM prod_trusted_gold.session.v_hotel_session_metrics
WHERE dt BETWEEN '[prev_week_start]' AND '[week_end]'
GROUP BY 1, 2
ORDER BY ABS(sessions_this - sessions_prev) DESC
LIMIT 30
```

### Step 6: Check bidding activity for eCPC drivers

**`prod_trusted_gold.hotel_commercial_partner_analytics.auction_insights_agg` contains all bidding activity.** A partner appears in this table only for markets where they have bidding open (CPC model). Markets where bidding is not yet open operate on CPA and will not appear here.

```sql
SELECT
  partner_name,
  SUM(CASE WHEN date BETWEEN '[week_start]' AND '[week_end]' THEN total_bid ELSE 0 END) AS total_bid_this,
  SUM(CASE WHEN date BETWEEN '[prev_week_start]' AND '[prev_week_end]' THEN total_bid ELSE 0 END) AS total_bid_prev,
  SUM(CASE WHEN date BETWEEN '[week_start]' AND '[week_end]' THEN clicks ELSE 0 END) AS clicks_this,
  SUM(CASE WHEN date BETWEEN '[prev_week_start]' AND '[prev_week_end]' THEN clicks ELSE 0 END) AS clicks_prev
FROM prod_trusted_gold.hotel_commercial_partner_analytics.auction_insights_agg
WHERE date BETWEEN '[prev_week_start]' AND '[week_end]'
GROUP BY 1
ORDER BY total_bid_this DESC
LIMIT 20
```

Average bid = `total_bid / clicks`. Compare WoW per partner.

**eCPC attribution framework:**

- **Bidding markets (CPC)**: revenue change in markets where the partner has bidding open is explained by bid changes. If `avg_bid` is up → eCPC up; down → eCPC down.
- **Non-bidding markets (CPA)**: revenue/redirect change in markets where the partner has not yet opened bidding comes from CPA revenue. The driver is most likely **higher H2** (redirect→booking conversion rate); higher booking value is a secondary possibility.

### Step 7: Generate charts and HTML report

After computing all numbers and writing the narrative, generate a self-contained HTML report with embedded charts. Use the Bash tool to run a Python script:

```python
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import numpy as np
import base64, pathlib, collections

BG = '#FAFAFA'

# ── Chart 1: Session WoW contribution by traffic source ──────────────────
# traffic_data = list of (source_name, sessions_this, sessions_prev)
# Filter to |delta| >= 5000, sort ascending (most negative at top)
# Bar labels: use K (thousands) not M, and include WoW% — e.g. "+95K (+17.4%)" or "−38K (−7.9%)"
def make_session_chart(traffic_data, week_label, out_path):
    sources = [t[0] for t in traffic_data]
    this_w  = np.array([t[1] for t in traffic_data])
    prev_w  = np.array([t[2] for t in traffic_data])
    delta   = this_w - prev_w
    mask    = np.abs(delta) >= 5_000
    src_f   = [sources[i] for i in range(len(sources)) if mask[i]]
    dlt_f   = delta[mask]
    prev_f  = prev_w[mask]
    order   = np.argsort(dlt_f)
    src_f   = [src_f[i] for i in order]
    dlt_f   = dlt_f[order]
    colors  = ['#E63946' if d < 0 else '#2DC653' for d in dlt_f]

    fig, ax = plt.subplots(figsize=(10, max(5, len(src_f) * 0.45)))
    fig.patch.set_facecolor(BG); ax.set_facecolor(BG)
    bars = ax.barh(src_f, dlt_f / 1e6, color=colors, height=0.6, zorder=3)
    ax.axvline(0, color='#333', linewidth=0.8, zorder=4)
    ax.grid(axis='x', linestyle='--', linewidth=0.5, alpha=0.5, zorder=2)
    for bar, val in zip(bars, dlt_f):
        x = bar.get_width()
        label = f'+{val/1e6:.2f}M' if val >= 0 else f'{val/1e6:.2f}M'
        ax.text(x + (0.03 if x >= 0 else -0.03), bar.get_y() + bar.get_height()/2,
                label, va='center', ha='left' if x >= 0 else 'right', fontsize=8)
    total = delta.sum(); prev_total = prev_w.sum()
    sign = '+' if total >= 0 else ''
    ax.text(0.99, 0.02, f'Total: {sign}{total/1e6:.2f}M ({100*total/prev_total:.1f}% WoW)',
            transform=ax.transAxes, ha='right', va='bottom', fontsize=9, color='#6C757D')
    ax.set_xlabel('Session change (millions)', fontsize=10)
    ax.set_title(f'Session WoW contribution by traffic source\n{week_label}', fontsize=12, fontweight='bold', pad=10)
    plt.tight_layout()
    plt.savefig(out_path, dpi=150, bbox_inches='tight'); plt.close()

# ── Chart 2: Dual-panel R/S — absolute (left) + WoW% (right) ────────────────
# Two side-by-side panels sharing the y-axis (traffic sources sorted by WoW% ascending).
# Left panel:  grouped horizontal bars — blue = this week R/S%, grey = prev week R/S%
# Right panel: green/red horizontal bars showing WoW% change in R/S (capped at ±100%)
# rs_data = list of (source, sessions_this, sessions_prev, redirects_this, redirects_prev)
# Filter to sources with sessions > 5000 both weeks before passing in.
def make_rs_chart(rs_data, week_label, out_path):
    CLIP = 100
    data = []
    for src, s_this, s_prev, r_this, r_prev in rs_data:
        if s_this > 0 and s_prev > 0:
            rs_t = r_this / s_this * 100
            rs_p = r_prev / s_prev * 100
            wow  = (rs_t - rs_p) / rs_p * 100 if rs_p > 0 else 0
            data.append((src, rs_t, rs_p, wow))
    data.sort(key=lambda x: x[3])  # ascending WoW%
    sources      = [d[0] for d in data]
    rs_this_vals = [d[1] for d in data]
    rs_prev_vals = [d[2] for d in data]
    rs_wows      = [d[3] for d in data]

    n = len(sources)
    y = np.arange(n)
    clipped    = [max(-CLIP, min(CLIP, w)) for w in rs_wows]
    wow_colors = ['#E63946' if w < 0 else '#2DC653' for w in rs_wows]

    fig, (ax_abs, ax_wow) = plt.subplots(
        1, 2, figsize=(15, max(5, n * 0.42)),
        sharey=True, gridspec_kw={'wspace': 0.04, 'width_ratios': [1, 1]}
    )
    fig.patch.set_facecolor(BG)

    # Left: absolute R/S this vs prev week
    ax_abs.set_facecolor(BG)
    ax_abs.barh(y - 0.18, rs_this_vals, 0.34, color='#1565C0', alpha=0.85, label='This week', zorder=3)
    ax_abs.barh(y + 0.18, rs_prev_vals, 0.34, color='#90A4AE', alpha=0.75, label='Prev week', zorder=3)
    ax_abs.set_yticks(y); ax_abs.set_yticklabels(sources, fontsize=8)
    ax_abs.set_xlabel('Redirects / Session (%)', fontsize=10)
    ax_abs.set_title('R/S — Absolute value', fontsize=11, fontweight='bold', pad=8)
    ax_abs.grid(axis='x', ls='--', lw=0.5, alpha=0.5, zorder=2)
    ax_abs.legend(fontsize=8, loc='lower right')
    max_abs = max(rs_this_vals + rs_prev_vals) if rs_this_vals else 1
    ax_abs.set_xlim(0, max_abs * 1.35)
    for i, tv in enumerate(rs_this_vals):
        ax_abs.text(tv + max_abs * 0.01, i - 0.18, f'{tv:.1f}%',
                    va='center', ha='left', fontsize=7, color='#1565C0')

    # Right: WoW% change
    ax_wow.set_facecolor(BG)
    ax_wow.barh(y, clipped, 0.6, color=wow_colors, zorder=3)
    ax_wow.axvline(0, color='#333', lw=0.8, zorder=4)
    ax_wow.set_xlabel('R/S WoW change (%)', fontsize=10)
    ax_wow.set_title('R/S — WoW % change', fontsize=11, fontweight='bold', pad=8)
    ax_wow.grid(axis='x', ls='--', lw=0.5, alpha=0.5, zorder=2)
    ax_wow.set_xlim(-CLIP - 15, CLIP + 15)
    for i, (cv, ov) in enumerate(zip(clipped, rs_wows)):
        is_clipped = abs(ov) > CLIP
        sign = '+' if ov >= 0 else ''
        lbl  = f'{sign}{ov:.0f}%{"*" if is_clipped else ""}'
        ax_wow.text(cv + (1.5 if cv >= 0 else -1.5), i, lbl,
                    va='center', ha='left' if cv >= 0 else 'right',
                    fontsize=8, fontweight='bold' if is_clipped else 'normal',
                    color='#1A237E' if cv >= 0 else '#B71C1C')
    if any(abs(ov) > CLIP for ov in rs_wows):
        ax_wow.text(0.99, 0.01, '* bar capped at ±100%',
                    transform=ax_wow.transAxes, ha='right', va='bottom',
                    fontsize=7.5, color='#6C757D', style='italic')

    fig.suptitle(f'Redirects/Session by traffic source\n{week_label}',
                 fontsize=12, fontweight='bold', y=1.01)
    plt.tight_layout()
    plt.savefig(out_path, dpi=150, bbox_inches='tight'); plt.close()

# ── Chart 3: Partner eCPC daily trend ────────────────────────────────────
# daily_data = list of (date_str 'YYYY-MM-DD', partner_id, gen_rev, redirects)
# partner_map = {'h_bc': 'Booking.com', 'h_ad': 'Agoda', ...}
# colors_map  = {'h_bc': '#003580', ...}
def make_ecpc_trend_chart(daily_data, partner_map, colors_map, prev_start, prev_end,
                           this_start, this_end, out_path):
    from collections import defaultdict
    daily = defaultdict(lambda: defaultdict(lambda: [0.0, 0]))
    for dt, pid, rev, redir in daily_data:
        daily[dt][pid][0] += rev; daily[dt][pid][1] += redir
    dates = sorted(daily.keys())

    fig, ax = plt.subplots(figsize=(12, 5))
    fig.patch.set_facecolor(BG); ax.set_facecolor(BG)
    for pid, name in partner_map.items():
        ecpcs, valid_dates = [], []
        for dt in dates:
            rev, redir = daily[dt][pid]
            if redir > 0 and rev / redir >= 0.2:
                ecpcs.append(rev / redir); valid_dates.append(dt)
        if ecpcs:
            xs = [dates.index(d) for d in valid_dates]
            ax.plot(xs, ecpcs, marker='o', markersize=4, linewidth=2,
                    color=colors_map.get(pid, '#888'), label=name, zorder=3)

    ax.set_xticks(range(len(dates)))
    ax.set_xticklabels([d[5:] for d in dates], rotation=45, ha='right', fontsize=8)
    if prev_start in dates and prev_end in dates:
        ax.axvspan(dates.index(prev_start) - 0.5, dates.index(prev_end) + 0.5,
                   alpha=0.07, color='blue', zorder=1)
    if this_start in dates and this_end in dates:
        ax.axvspan(dates.index(this_start) - 0.5, dates.index(this_end) + 0.5,
                   alpha=0.07, color='orange', zorder=1)
    ax.set_ylabel('eCPC (£ / redirect)', fontsize=10)
    ax.set_title('Partner eCPC daily trend\n(generated revenue / redirects)', fontsize=12, fontweight='bold', pad=10)
    ax.legend(loc='upper right', fontsize=9, framealpha=0.8)
    ax.grid(axis='y', linestyle='--', linewidth=0.5, alpha=0.5, zorder=2)
    plt.tight_layout()
    plt.savefig(out_path, dpi=150, bbox_inches='tight'); plt.close()

# ── Chart 4: Partner eCPC WoW% bar chart ─────────────────────────────────
# partner_weekly = list of (name, color, rev_this, red_this, rev_prev, red_prev)
def make_ecpc_wow_chart(partner_weekly, week_label, out_path):
    names, wow, colors = [], [], []
    for name, color, rev_this, red_this, rev_prev, red_prev in partner_weekly:
        if red_this > 0 and red_prev > 0:
            wow_pct = ((rev_this/red_this) - (rev_prev/red_prev)) / (rev_prev/red_prev) * 100
            names.append(name); wow.append(wow_pct); colors.append(color)
    order  = np.argsort(wow)
    names  = [names[i]  for i in order]
    wow    = [wow[i]    for i in order]
    colors = [colors[i] for i in order]

    fig, ax = plt.subplots(figsize=(8, max(3, len(names) * 0.6)))
    fig.patch.set_facecolor(BG); ax.set_facecolor(BG)
    bars = ax.barh(names, wow, color=colors, height=0.5, zorder=3)
    ax.axvline(0, color='#333', linewidth=0.8, zorder=4)
    ax.grid(axis='x', linestyle='--', linewidth=0.5, alpha=0.5, zorder=2)
    for bar, val in zip(bars, wow):
        x = bar.get_width()
        label = f'+{val:.1f}%' if val >= 0 else f'{val:.1f}%'
        ax.text(x + (0.1 if x >= 0 else -0.1), bar.get_y() + bar.get_height()/2,
                label, va='center', ha='left' if x >= 0 else 'right',
                fontsize=9, fontweight='bold')
    ax.set_xlabel('eCPC WoW change (%)', fontsize=10)
    ax.set_title(f'Partner eCPC WoW change\n{week_label}', fontsize=12, fontweight='bold', pad=10)
    plt.tight_layout()
    plt.savefig(out_path, dpi=150, bbox_inches='tight'); plt.close()

# ── HTML assembly ─────────────────────────────────────────────────────────
def img_b64(path):
    return base64.b64encode(pathlib.Path(path).read_bytes()).decode()

def build_html(report_data, chart_paths, out_path):
    """
    report_data keys:
      week_start, week_end, prev_week_start, prev_week_end,
      declared_rev_this, declared_rev_prev, declared_rev_yoy,
      gen_rev_this, gen_rev_prev, gen_rev_yoy,
      redirects_this, redirects_prev, redirects_yoy,
      sessions_this, sessions_prev, sessions_yoy,
      highlight_text,
      sessions_bullets (list of str),
      rs_bullets (list of str),
      rev_redirect_bullets (list of str),
      sessions_rag, rs_rag, rev_redirect_rag  (emoji: 🟢 / 🟠 / 🔴)
    chart_paths keys: sessions, rs, ecpc_wow, ecpc_trend
    All 7 metrics must have both WoW and YoY in the HTML topline table.
    """
    d = report_data
    def pct(a, b): return f'{(a-b)/b*100:+.1f}%'
    def fmt_gbp(v): return f'£{v:,.0f}'

    sessions_wow = pct(d['sessions_this'], d['sessions_prev'])
    redirects_wow = pct(d['redirects_this'], d['redirects_prev'])
    rs_this = d['redirects_this'] / d['sessions_this']
    rs_prev = d['redirects_prev'] / d['sessions_prev']
    rs_wow  = pct(rs_this, rs_prev)
    decl_wow = pct(d['declared_rev_this'], d['declared_rev_prev'])
    decl_yoy = pct(d['declared_rev_this'], d['declared_rev_yoy'])
    gen_wow  = pct(d['gen_rev_this'], d['gen_rev_prev'])
    ecpc_decl_this = d['declared_rev_this'] / d['redirects_this']
    ecpc_decl_prev = d['declared_rev_prev'] / d['redirects_prev']
    ecpc_decl_yoy  = d['declared_rev_yoy']  / d['redirects_yoy']
    ecpc_decl_wow  = pct(ecpc_decl_this, ecpc_decl_prev)
    ecpc_decl_yoy_pct = pct(ecpc_decl_this, ecpc_decl_yoy)
    ecpc_gen_this  = d['gen_rev_this']  / d['redirects_this']
    ecpc_gen_prev  = d['gen_rev_prev']  / d['redirects_prev']
    ecpc_gen_wow   = pct(ecpc_gen_this, ecpc_gen_prev)

    def color_class(s):
        if s.startswith('+') and s != '+0.0%': return 'pos'
        if s.startswith('-'): return 'neg'
        return 'neu'

    def bullets_html(items):
        return ''.join(f'<li>{item}</li>' for item in items)

    c = {k: img_b64(v) for k, v in chart_paths.items()}

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Hotels WBR – {d['week_start']} to {d['week_end']}</title>
<style>
  *{{box-sizing:border-box;margin:0;padding:0}}
  body{{font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,sans-serif;font-size:14px;line-height:1.6;color:#172B4D;background:#F4F5F7;padding:32px 20px}}
  .page{{max-width:900px;margin:0 auto;background:#fff;border-radius:8px;box-shadow:0 1px 4px rgba(0,0,0,.12);padding:40px 48px}}
  .report-header{{border-bottom:3px solid #0052CC;padding-bottom:16px;margin-bottom:28px}}
  .report-header .brand{{font-size:11px;font-weight:600;letter-spacing:.08em;text-transform:uppercase;color:#0052CC;margin-bottom:4px}}
  .report-header h1{{font-size:24px;font-weight:700;color:#172B4D}}
  .report-header .date-range{{font-size:13px;color:#6B778C;margin-top:4px;font-style:italic}}
  .highlight{{background:#E6EFFC;border-left:4px solid #0052CC;border-radius:4px;padding:16px 20px;margin-bottom:32px}}
  .highlight .label{{font-size:11px;font-weight:700;letter-spacing:.1em;text-transform:uppercase;color:#0052CC;margin-bottom:8px}}
  .highlight p{{color:#172B4D;font-size:14px;line-height:1.65}}
  .metric{{margin-bottom:36px}}
  .metric-header{{display:flex;align-items:baseline;gap:10px;margin-bottom:12px;padding-bottom:8px;border-bottom:1px solid #EBECF0}}
  .rag{{font-size:20px;line-height:1}}
  .metric-title{{font-size:17px;font-weight:700;color:#172B4D}}
  .metric-title .wow{{font-size:15px;font-weight:600;color:#172B4D}}
  .metric-title .yoy{{font-size:13px;font-weight:400;color:#6B778C;margin-left:4px}}
  .metric ul{{margin:10px 0 10px 20px}}
  .metric ul li{{margin-bottom:6px;color:#172B4D}}
  code{{background:#F4F5F7;border:1px solid #DFE1E6;border-radius:3px;padding:1px 5px;font-size:12px;font-family:"SFMono-Regular",Consolas,monospace;color:#172B4D}}
  .chart-wrap{{margin:16px 0;text-align:center}}
  .chart-wrap img{{max-width:100%;border:1px solid #EBECF0;border-radius:6px}}
  .chart-caption{{font-size:11.5px;color:#6B778C;margin-top:5px;font-style:italic}}
  .kpi-table{{width:100%;border-collapse:collapse;margin-bottom:32px;font-size:13px}}
  .kpi-table th{{background:#0052CC;color:#fff;padding:9px 14px;text-align:left;font-weight:600}}
  .kpi-table td{{padding:8px 14px;border-bottom:1px solid #EBECF0}}
  .kpi-table tr:nth-child(even) td{{background:#F4F5F7}}
  .kpi-table .num{{text-align:right;font-variant-numeric:tabular-nums}}
  .pos{{color:#006644;font-weight:600}}.neg{{color:#BF2600;font-weight:600}}.neu{{color:#172B4D}}
  .section-title{{font-size:13px;font-weight:700;letter-spacing:.07em;text-transform:uppercase;color:#6B778C;margin:32px 0 16px;padding-bottom:4px;border-bottom:2px solid #EBECF0}}
  .footer{{margin-top:40px;padding-top:16px;border-top:1px solid #EBECF0;font-size:11.5px;color:#97A0AF}}
</style>
</head>
<body>
<div class="page">
  <div class="report-header">
    <div class="brand">Skyscanner · PAC Tribe · WBR</div>
    <h1>Hotels KPI Scorecard</h1>
    <div class="date-range">{d['week_start']} – {d['week_end']}</div>
  </div>

  <div class="section-title">Topline Summary</div>
  <table class="kpi-table">
    <thead><tr><th>Metric</th><th class="num">This Week</th><th class="num">Prev Week</th><th class="num">WoW</th><th class="num">YoY</th></tr></thead>
    <tbody>
      <tr><td>Declared Revenue (£)</td><td class="num">{fmt_gbp(d['declared_rev_this'])}</td><td class="num">{fmt_gbp(d['declared_rev_prev'])}</td><td class="num {color_class(decl_wow)}">{decl_wow}</td><td class="num {color_class(decl_yoy)}">{decl_yoy}</td></tr>
      <tr><td>Generated Revenue (£)</td><td class="num">{fmt_gbp(d['gen_rev_this'])}</td><td class="num">{fmt_gbp(d['gen_rev_prev'])}</td><td class="num {color_class(gen_wow)}">{gen_wow}</td><td class="num">—</td></tr>
      <tr><td>Sessions</td><td class="num">{d['sessions_this']:,}</td><td class="num">{d['sessions_prev']:,}</td><td class="num {color_class(sessions_wow)}">{sessions_wow}</td><td class="num">—</td></tr>
      <tr><td>Redirects</td><td class="num">{d['redirects_this']:,}</td><td class="num">{d['redirects_prev']:,}</td><td class="num {color_class(redirects_wow)}">{redirects_wow}</td><td class="num {color_class(pct(d['redirects_this'], d['redirects_yoy']))}">{ pct(d['redirects_this'], d['redirects_yoy'])}</td></tr>
      <tr><td>Redirects / Session</td><td class="num">{rs_this*100:.2f}%</td><td class="num">{rs_prev*100:.2f}%</td><td class="num {color_class(rs_wow)}">{rs_wow}</td><td class="num">—</td></tr>
      <tr><td>Declared eCPC (£)</td><td class="num">£{ecpc_decl_this:.3f}</td><td class="num">£{ecpc_decl_prev:.3f}</td><td class="num {color_class(ecpc_decl_wow)}">{ecpc_decl_wow}</td><td class="num {color_class(ecpc_decl_yoy_pct)}">{ecpc_decl_yoy_pct}</td></tr>
      <tr><td>Generated eCPC (£)</td><td class="num">£{ecpc_gen_this:.3f}</td><td class="num">£{ecpc_gen_prev:.3f}</td><td class="num {color_class(ecpc_gen_wow)}">{ecpc_gen_wow}</td><td class="num">—</td></tr>
    </tbody>
  </table>

  <div class="highlight">
    <div class="label">✦ Highlight of the Week</div>
    <p>{d['highlight_text']}</p>
  </div>

  <div class="section-title">Overview of Hotel Metrics WoW</div>

  <div class="metric">
    <div class="metric-header">
      <span class="rag">{d['sessions_rag']}</span>
      <span class="metric-title">Sessions <span class="wow">{sessions_wow} WoW</span></span>
    </div>
    <ul>{bullets_html(d['sessions_bullets'])}</ul>
    <div class="chart-wrap">
      <img src="data:image/png;base64,{c['sessions']}" alt="Session contribution chart">
      <div class="chart-caption">Session WoW contribution (absolute) by traffic source</div>
    </div>
  </div>

  <div class="metric">
    <div class="metric-header">
      <span class="rag">{d['rs_rag']}</span>
      <span class="metric-title">Redirects/Session <span class="wow">{rs_wow} WoW</span></span>
    </div>
    <ul>{bullets_html(d['rs_bullets'])}</ul>
    <div class="chart-wrap">
      <img src="data:image/png;base64,{c['rs']}" alt="Redirects/Session chart">
      <div class="chart-caption">Left: R/S absolute value (blue = this week, grey = prev week). Right: R/S WoW change (%) by traffic source. Bars capped at ±100%; starred values show actual magnitude.</div>
    </div>
  </div>

  <div class="metric">
    <div class="metric-header">
      <span class="rag">{d['rev_redirect_rag']}</span>
      <span class="metric-title">Revenue/Redirect <span class="wow">{ecpc_decl_wow} WoW (declared) / {ecpc_gen_wow} WoW (generated)</span>
        <span class="yoy">{ecpc_decl_yoy_pct} YoY</span>
      </span>
    </div>
    <ul>{bullets_html(d['rev_redirect_bullets'])}</ul>
    <div class="chart-wrap">
      <img src="data:image/png;base64,{c['ecpc_wow']}" alt="Partner eCPC WoW chart">
      <div class="chart-caption">Partner eCPC WoW change (%)</div>
    </div>
    <div class="chart-wrap" style="margin-top:20px">
      <img src="data:image/png;base64,{c['ecpc_trend']}" alt="Partner eCPC trend chart">
      <div class="chart-caption">Partner eCPC daily trend (generated revenue / redirects). Shaded: blue = prev week, orange = report week.</div>
    </div>
  </div>

  <div class="footer">
    Generated by Claude Code · Data: Databricks prod_trusted_gold ·
    Report week: {d['week_start']} – {d['week_end']} ·
    Prev week: {d['prev_week_start']} – {d['prev_week_end']}
  </div>
</div>
</body>
</html>"""

    pathlib.Path(out_path).write_text(html, encoding='utf-8')
    print(f"HTML report saved: {out_path}")
```

**How to use these functions:**
1. After all queries return data, populate the input dicts/lists from query results
2. Call each `make_*_chart()` function to save PNGs to a temp directory
3. Call `build_html()` with the computed `report_data` dict and chart paths to produce the final `.html` file
4. Report the output file path to the user so they can open it in a browser or upload to Confluence

**Output file naming convention:** `hotel_wbr_[week_start]_[week_end].html` (e.g. `hotel_wbr_2026-03-20_2026-03-26.html`), saved to the current working directory.

## Writing the Callouts

Once you have the numbers, write the callouts following the example style from the live WBR. The patterns below are drawn directly from real WBR pages.

### Highlight paragraph style

Lead with both declared and generated revenue movements, then explain using the revenue equation chain. Examples:

> "Declared revenue and generated revenue experienced small drops, down by 1.9% and 1.5% respectively, after 10.2% drop in sessions was counteracted by 8.6% increase in redirect/session, and revenue/redirect (+0.5%) stayed stable."
> — WBR-24MAR2026 (13–19 MAR 2026)

> "Declared revenue went down by 9.3%, and generated revenue went down by 12.2% owing to 15.6% drop in sessions despite 7.4% increase in redirects/sessions. Revenue/redirects (+0.0%) meanwhile stayed stable."
> — WBR-30MAR2026 (20–26 MAR 2026)

Pattern: *[Revenue direction + declared% + generated%] + [Sessions direction, but counteracted/driven by R/S direction] + [eCPC was stable / also changed]*

**When declared and generated diverge significantly**, use separate bold header lines before the explanation (especially when a data issue is involved):

> **Generated Revenue (-4.1% WoW / -10% YoY)**
> **Declared Revenue (+0.4% WoW / -12.0% YoY)**
> Declared revenue is nearly flat WoW. This is driven by: lower number of sessions (-2.1% WoW) / slight drop in redirect/session (-1.0% WoW) / which is being offset by positive change in eCPC (+3.6% WoW)
> — WBR-24FEB2026 (13–19 FEB 2026)

> "Declared revenue (-0.4%) stayed stable due to 10.1% drop in redirect/sessions counteracting 11.2% growth in sessions and also stable revenue/redirect (-0.4%). Generated revenue went down by 13.4% owing to a bug in the hotel booking parser that stopped CPA revenue data flow."
> — WBR-17MAR2026 (6–12 MAR 2026)

When a pipeline/data bug explains the divergence, name the bug explicitly in the highlight — don't bury it in the eCPC section alone.

### Sessions callout style

1. **Experiment-driven anomalies first.** If a traffic source spike or drop is explained by an experiment ending or starting, state that explicitly using the past-tense framing: *"No longer grouped under _xsell_flight_homepage_, parallel_search sessions decreased by 11.4%"*
2. **Isolation pattern.** When one or more sources dominate the headline (especially experiment-driven ones), qualify the rest by excluding them: *"Excluding parallel_search related traffic, total sessions increased by 1.6%"* or with multiple sources: *"Excluding _internal_other_, _parallel_search_, and _direct non-homepage_, total sessions decreased by 7%."* Use this to expose the underlying organic trend when experiment traffic inflates or deflates the headline.
3. **Country attribution.** Always drill into which market drove the change: *"The rise in _non-brand SEM_ sessions was driven by 44% session growth in India"*
4. **Paid HPA market breakdown.** Whenever _Paid HPA_ sessions move notably, always list the key markets with their individual WoW%: *"Though _paid_HPA_ sessions stayed relatively stable, big changes were seen in different markets. India sessions continued to decline, down 12%, while Brazil rose by 12% and Taiwan rose by 42%."*
5. **News/context link.** If a market spike has a real-world cause (airline surcharges, news event), briefly note it.
6. **Traffic source names in italics** in the Confluence/markdown text: `_direct_homepage_`, `_internal_other_`, `_Paid HPA_`, `_non-brand SEM_`, `_organic_`, `_parallel_search_`, etc.

### Redirect/Session callout style

1. **Explain mix-shift explicitly.** If R/S improved because low-converting traffic was lost (e.g., experiment end), say: *"_internal_other_ redirect/session (up 62.8%) went back to the pre-experiment level as the experiment ended"*
2. **Proportionality framing with numbers.** When session growth doesn't bring proportional redirect uplift, quantify the gap explicitly: *"_internal_other_ redirect only increased by 32% following the 83% increase in its sessions, leading to 28% drop in its redirect/session."* Always include both the session growth% and the redirect growth% to make the shortfall concrete.
3. **Chain the R/S driver.** Give the source-level breakdown: *"_Paid_HPA_ redirect/session decreased by 3.9% as the redirect went down by 4.9% after 1.1% decline in its sessions"*

### Revenue/Redirect callout style

1. **Open with generated revenue/redirect** since eCPC uses generated revenue: *"Using generated revenue, revenue/redirect (+1%) had a similar change"* (or note when it diverges significantly from declared).
2. **Summarise major partners in one line.** Name all top-3 partners with direction and size: *"Major partners had small WoW changes in eCPC (Agoda up 2%, Bcom up 1.2%, and Trip down 0.1%)"*
3. **eCPC flat + redirects down → revenue explicitly lower.** When a partner's eCPC didn't change but volume fell, say so: *"Agoda and Bcom eCPC stayed unchanged, and their revenue went down by 6% and 10%, respectively."* Don't just report eCPC and leave the revenue implication implicit.
4. **Forward-looking bid signal.** If partners started changing bids mid-week, note it as context: *"they'd started to drop their bids since the weekend"*
5. **Market-level bid attribution.** For partners with notable eCPC moves, name the specific markets where bids changed (from `auction_insights_agg`): *"Lastminute was providing more competitive prices and bidding higher in Spain (eCPC up 84%) and Germany (eCPC up 70%). This led to 80% increase in its overall redirect and 108% increase in its revenue"* or *"AU, DE and FR see drops in Avg. Bids"*
6. **CPA/competitive pricing attribution.** For CPA partners, when revenue jumps, attribute to pricing competitiveness: *"MMT generated revenue increased by 122% after it started to provide more competitive prices in India"*
7. **Partner revenue chain.** For notable movers, connect eCPC → redirects → revenue: *"Lastminute eCPC continued to increase (+9.9%), leading to a 25% increase in its redirect and 38% increase in its revenue"*
8. **Bid recovery context.** If a partner dropped then recovered, note both: *"Vio dropped its bid early last week, and its eCPC decreased by 17.9%, though its eCPC has recovered after higher bids since this week"*
9. **Data quality/pipeline bug callout.** If a data pipeline issue affected CPA revenue, call it out explicitly with the date range and whether it has been fixed: *"A bug in the hotel scraper stopped CPA revenue from flowing into the system since [date]; this was fixed, and the missing data would be populated soon."* Name which partners were affected (e.g., Expedia, Hotels.com CPA).

## Reference WBR Pages

These real WBR pages were used to define the writing style above. Read them for additional context if needed:

- **WBR-24FEB2026** (13–19 FEB 2026): https://skyscanner.atlassian.net/wiki/x/MwGEgQ  
  — separate-header highlight format for diverging declared/generated, multi-source exclusion, Paid HPA market breakdown, market-level bid attribution (AU/DE/FR bids down), bid recovery signal
- **WBR-3MAR2026** (24 FEB – 2 MAR 2026): https://skyscanner.atlassian.net/wiki/x/AYBXgw  
  — parallel_search exclusion still yielding positive result, Paid HPA market breakdown (UK/ES/AU/MX/JP), Lastminute eCPC recovery → revenue chain
- **WBR-10MAR2026** (3–9 MAR 2026): https://skyscanner.atlassian.net/wiki/x/8bOahQ  
  — multi-source exclusion (internal_other + parallel_search + direct non-homepage), eCPC flat + revenue down pattern, CPA/competitive pricing attribution (MMT India)
- **WBR-17MAR2026** (6–12 MAR 2026): https://skyscanner.atlassian.net/wiki/x/Iatnhw  
  — data quality/pipeline bug in highlight (CPA revenue gap), quantified proportionality in R/S (32% redirect vs 83% session), market-level bid attribution for small partner (Lastminute Spain/Germany)
- **WBR-24MAR2026** (13–19 MAR 2026): https://skyscanner.atlassian.net/wiki/x/SJe8Vg  
  — parallel_search experiment end, India non-brand SEM spike, Lastminute/Vio eCPC movements, forward-looking bid drop signal
- **WBR-30MAR2026** (20–26 MAR 2026): prior skill training example  
  — xsell_flight_homepage experiment end (-98.8%), mix-shift R/S lift, Agoda eCPC drag

## Key Concepts

- **Declared revenue** ≈ finance-settled view (can include cancellations/adjustments). Used for headline revenue.
- **Generated revenue** ≈ attributed at booking time (redirects × eCPC). Better for near-term eCPC analysis.
- For **redirect revenue partners** (CPC model), declared ≈ generated. Difference is mostly in **booking revenue** partners (CPA model).
- **eCPC** = Generated Revenue / Redirect. This is what Revenue/Redirect refers to in the eCPC context.
- **H2** = Booking/Redirect conversion rate (relevant for CPA partners).
- Report focuses on **WoW** (week over week) as the primary comparison; **YoY** as secondary context.
- `website_id = 'h_hc'` should be treated as `h_h1` (Hotels.com alias).

## Notes on Interpretation

- Large swings in `internal_other` or `parallel_search` sessions often reflect experiment starts/ends — always check if an experiment could explain anomalies.
- Suspicious session spikes in small/unusual markets (e.g., Venezuela, Nigeria, Bolivia) may indicate bot traffic.
- YoY comparisons can be misleading if last year had a one-time event (marketing spike, bot traffic, experiment) — note this when relevant.
- When a major source dominates the session headline (e.g., experiment end), use the "excluding X" pattern to expose the underlying organic trend.
- When R/S improves after low-converting traffic drops, call it out as "mix-shift" rather than a genuine R/S improvement — it means underlying conversion didn't change.
- Check auction_insights_agg for bid-level context when eCPC moves significantly — distinguishing bid changes from volume changes explains whether eCPC movement is structural (bid strategy) or mechanical (mix shift).
- For Paid HPA: `paid_hpa_google_hpa` (R/S ~53%) and `paid_hpa_google_ppa` (R/S ~17%) have very different R/S profiles. A mix shift between them can move overall Paid HPA R/S without any change in underlying bid quality. Use `revenue_referral_event` with `utm_source = 'google_paid'` and `RIGHT(utm_campaign, 3) IN ('HPA', 'PPA')` to split them when needed.
- When sessions drop, also check if redirect/session moved inversely (low-converting traffic mix change).
