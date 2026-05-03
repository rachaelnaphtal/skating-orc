import datetime
import html as html_module
import pandas as pd


REPORT_FEEDBACK_EMAIL = "xyx@email.com"

# Tooltip copy for summary stat cards (embedded as HTML text, escaped).
TOOLTIP_THROWOUT_RATE = html_module.escape(
    "This is the percentage of time that you are the high or low of the panel. "
    "If at least three judges give the same score then it will not count as thrown out.",
)
TOOLTIP_ANOMALY_RATE = html_module.escape(
    "Anomalies are scores that are >=2 away from the panel average for GOEs "
    "and >=1.5 away from the panel average for PCS.",
)
TOOLTIP_RULE_ERROR_RATE = html_module.escape(
    "Marks flagged as impossible under the published judging guidelines for the segment.",
)
TOOLTIP_TOTAL_SCORES = html_module.escape(
    "Count of individual scores in this report after applied filters.",
)


def _filters_block_html(filter_summary_lines):
    if not filter_summary_lines:
        return ""
    items = "".join(
        f"<li>{html_module.escape(str(line))}</li>" for line in filter_summary_lines
    )
    return (
        '<div class="filters-used">'
        '<div class="filters-title">Filters applied (from report generator)</div>'
        f"<ul>{items}</ul></div>"
    )


def _report_instructions_html():
    return f"""
<div class="report-instructions">
  <h2 class="instructions-heading">How to use this report</h2>
  <p>This report is designed to give you insights into your judging marks, allowing you to explore how your marks compare to other judges and areas to prioritize in your preparation for future events.</p>
  <p><strong>We recommend prioritizing analysis as follows:</strong></p>
  <ul>
    <li><strong>Rule Error Rate:</strong> These represent marks that are not possible based on the judging guidelines (i.e., NHT -5 for a +COMBO in a Singles Short Program). Minimizing Rule Errors is critical to build trust and confidence in our judging panels.</li>
    <li><strong>Anomaly Rate:</strong> These represent marks considered potential errors in marking guideline application.</li>
    <li><strong>Throw Out Rate:</strong> These represent marks that are not considered potential errors but help assess where you may be too lenient or critical in marking guideline applications.</li>
  </ul>
  <p>Use the <strong>Element Breakdown</strong> and <strong>Segment Statistics</strong> tabs to identify patterns of Rule Errors, Anomalies and Throw Outs. This will help you narrow your focus for continuous learning.</p>
  <p>Use the <strong>Element Details</strong> and <strong>PCS Details</strong> tabs to pinpoint exact competitors, elements and components for targeted reflection.</p>
  <p class="feedback-line">Email <a href="mailto:{html_module.escape(REPORT_FEEDBACK_EMAIL)}">{html_module.escape(REPORT_FEEDBACK_EMAIL)}</a> to ask questions and provide feedback on the usefulness of this report.</p>
</div>
"""


def _html_table(table_id, headers, rows):
    if not headers:
        return '<p class="empty-msg">No data available.</p>'
    ths = ''.join(
        f'<th onclick="sortTable(\'{table_id}\',{i})">'
        f'{h}<span class="sort-icon">⇅</span></th>'
        for i, h in enumerate(headers))
    trs = ''
    for row in rows:
        tds = ''.join(f'<td>{v}</td>' for v in row)
        trs += f'<tr>{tds}</tr>'
    return (f'<table id="{table_id}"><thead><tr>{ths}</tr></thead>'
            f'<tbody>{trs}</tbody></table>')


def build_judge_report_html(
    judge_name,
    report_stats,
    report_pcs_df,
    report_elem_df,
    report_seg_df,
    *,
    single_competition_display_name=None,
    filter_summary_lines=None,
):

    # ── helpers ─────────────────────────────────────────────────────────────
    def get_issue_label(row):
        issues = []
        if row.get('thrown_out'):
            issues.append(f"Thrown Out ({'High' if row['deviation'] > 0 else 'Low'})")
        if row.get('anomaly'):
            issues.append(f"Anomaly ({'High' if row['deviation'] > 0 else 'Low'})")
        if row.get('is_rule_error'):
            issues.append('Rule Error')
        return ', '.join(issues)

    def df_to_rows(df, cols, rename):
        if df.empty:
            return [], []
        sub = df[cols].copy()
        sub.columns = rename
        for c in sub.columns:
            if sub[c].dtype in [float, 'float64']:
                sub[c] = sub[c].round(2)
        return list(sub.columns), [[str(v) for v in r]
                                   for r in sub.fillna('').values.tolist()]

    def compute_elem_breakdown(df, group_cols, label_cols):
        if df.empty:
            return [], []

        def agg(g):
            n = len(g)
            to = g['thrown_out'].sum()
            an = g['anomaly'].sum()
            re = g['is_rule_error'].sum()
            return pd.Series({
                'Total': n,
                'Throwouts': int(to),
                'TO High': int(((g['thrown_out']) & (g['deviation'] > 0)).sum()),
                'TO Low': int(((g['thrown_out']) & (g['deviation'] < 0)).sum()),
                'TO Rate (%)': round(to / n * 100, 1) if n else 0,
                'Anomalies': int(an),
                'Anom High': int(((g['anomaly']) & (g['deviation'] > 0)).sum()),
                'Anom Low': int(((g['anomaly']) & (g['deviation'] < 0)).sum()),
                'Anom Rate (%)': round(an / n * 100, 1) if n else 0,
                'Rule Errors': int(re),
                'RE Rate (%)': round(re / n * 100, 1) if n else 0,
            })

        result = df.groupby(group_cols).apply(
            agg, include_groups=False).reset_index()
        result.columns = label_cols + list(result.columns[len(group_cols):])
        return list(result.columns), [[str(v) for v in r]
                                      for r in result.fillna('').values.tolist()]

    # ── element breakdown tables ─────────────────────────────────────────────
    bk_type_h, bk_type_r = [], []
    bk_year_h, bk_year_r = [], []
    bk_comp_h, bk_comp_r = [], []
    if not report_elem_df.empty:
        bk_type_h, bk_type_r = compute_elem_breakdown(
            report_elem_df, ['element_type_name'], ['Element Type'])
        bk_year_h, bk_year_r = compute_elem_breakdown(
            report_elem_df, ['element_type_name', 'year'],
            ['Element Type', 'Year'])
        bk_comp_h, bk_comp_r = compute_elem_breakdown(
            report_elem_df, ['element_type_name', 'competition_name'],
            ['Element Type', 'Competition'])

    # ── element issues ───────────────────────────────────────────────────────
    elem_headers, elem_rows = [], []
    if not report_elem_df.empty:
        ei = report_elem_df[report_elem_df['thrown_out'] |
                            report_elem_df['anomaly'] |
                            report_elem_df['is_rule_error']].copy()
        if not ei.empty:
            ei['Issue Type'] = ei.apply(get_issue_label, axis=1)
            elem_headers, elem_rows = df_to_rows(
                ei,
                ['competition_name', 'year', 'segment_name', 'skater_name',
                 'element_name', 'element_type_name', 'judge_score',
                 'panel_average', 'deviation', 'Issue Type'],
                ['Competition', 'Year', 'Segment', 'Skater', 'Element',
                 'Element Type', 'Judge Score', 'Panel Avg', 'Deviation',
                 'Issue Type'])

    # ── PCS issues ───────────────────────────────────────────────────────────
    pcs_headers, pcs_rows = [], []
    if not report_pcs_df.empty:
        pi = report_pcs_df[report_pcs_df['thrown_out'] |
                           report_pcs_df['anomaly'] |
                           report_pcs_df['is_rule_error']].copy()
        if not pi.empty:
            pi['Issue Type'] = pi.apply(get_issue_label, axis=1)
            pcs_headers, pcs_rows = df_to_rows(
                pi,
                ['competition_name', 'year', 'segment_name', 'skater_name',
                 'pcs_type_name', 'judge_score', 'panel_average',
                 'deviation', 'Issue Type'],
                ['Competition', 'Year', 'Segment', 'Skater', 'PCS Component',
                 'Judge Score', 'Panel Avg', 'Deviation', 'Issue Type'])

    # ── segment stats ────────────────────────────────────────────────────────
    seg_headers, seg_rows = [], []
    if not report_seg_df.empty:
        seg_headers, seg_rows = df_to_rows(
            report_seg_df,
            ['competition_name', 'competition_year', 'discipline',
             'segment_name', 'skater_count', 'total_anomalies',
             'pcs_anomalies', 'element_anomalies', 'total_rule_errors',
             'pcs_rule_errors', 'element_rule_errors'],
            ['Competition', 'Year', 'Discipline', 'Segment', 'Skaters',
             'Total Anomalies', 'PCS Anomalies', 'Elem Anomalies',
             'Total Rule Errors', 'PCS Rule Errors', 'Elem Rule Errors'])

    today = datetime.date.today().strftime('%Y-%m-%d')

    if single_competition_display_name:
        doc_title = f"Judge Report- {single_competition_display_name}"
        h1_text = doc_title
    else:
        doc_title = f"Judge Report – {judge_name}"
        h1_text = "Judge Report"
    filters_html = _filters_block_html(filter_summary_lines or [])
    instructions_html = _report_instructions_html()
    safe_title = html_module.escape(doc_title)
    safe_h1 = html_module.escape(h1_text)
    safe_judge = html_module.escape(judge_name)

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>{safe_title}</title>
<style>
  *{{box-sizing:border-box;margin:0;padding:0}}
  body{{font-family:'Segoe UI',Arial,sans-serif;background:#f4f6fb;color:#222;padding:24px}}
  h1{{color:#1a3a5c;margin-bottom:4px}}
  .meta{{color:#666;font-size:0.9em;margin-bottom:12px}}
  .filters-used{{background:#fff;border-radius:8px;padding:14px 18px;margin-bottom:20px;
                 box-shadow:0 1px 4px rgba(0,0,0,0.06);border-left:4px solid #3d6ea8}}
  .filters-title{{font-weight:700;color:#1a3a5c;margin-bottom:8px;font-size:0.95em}}
  .filters-used ul{{margin:0 0 0 1.2em;padding:0;color:#333;line-height:1.5}}
  .report-instructions{{background:#fff;border-radius:8px;padding:20px 22px;margin-bottom:24px;
                        box-shadow:0 2px 8px rgba(0,0,0,0.06);line-height:1.55;color:#333}}
  .instructions-heading{{font-size:1.15em;color:#1a3a5c;margin:0 0 12px}}
  .report-instructions ul{{margin:8px 0 12px 1.2em}}
  .report-instructions p{{margin:0 0 12px}}
  .feedback-line{{margin-top:16px;margin-bottom:0;font-size:0.95em}}
  .feedback-line a{{color:#1a3a5c}}
  .tabs{{display:flex;gap:8px;margin-bottom:0;flex-wrap:wrap}}
  .tab{{padding:10px 22px;border:none;border-radius:6px 6px 0 0;cursor:pointer;
        background:#dde4f0;color:#444;font-size:0.95em;font-weight:600;transition:background 0.2s}}
  .tab.active{{background:#1a3a5c;color:#fff}}
  .panel{{display:none;background:#fff;border-radius:0 8px 8px 8px;padding:24px;
          box-shadow:0 2px 8px rgba(0,0,0,0.08);margin-bottom:32px;overflow:visible}}
  .panel.active{{display:block}}
  .stats-grid{{display:grid;grid-template-columns:repeat(auto-fit,minmax(200px,1fr));gap:16px;margin-bottom:8px;overflow:visible}}
  .stat-card{{background:#f0f4ff;border-radius:8px;padding:16px 20px;border-left:4px solid #1a3a5c;position:relative;overflow:visible;cursor:help}}
  .stat-card:focus{{outline:2px solid #3d6ea8;outline-offset:2px}}
  .stat-tip-bubble{{position:absolute;left:0;bottom:calc(100% + 8px);margin:0;
                  padding:11px 14px;background:#1a3a5c;color:#fff;font-size:0.84rem;
                  line-height:1.45;font-weight:400;text-transform:none;letter-spacing:normal;
                  border-radius:8px;box-shadow:0 6px 20px rgba(0,0,0,0.22);
                  width:max-content;max-width:min(360px,calc(100vw - 48px));
                  z-index:200;opacity:0;visibility:hidden;pointer-events:none;
                  transition:opacity 0.12s ease,visibility 0.12s ease}}
  .stat-card.stat-with-tip:hover .stat-tip-bubble,
  .stat-card.stat-with-tip:focus .stat-tip-bubble,
  .stat-card.stat-with-tip:focus-within .stat-tip-bubble{{opacity:1;visibility:visible}}
  .stat-card h3.stat-label{{font-size:0.8em;color:#555;text-transform:uppercase;letter-spacing:.05em;margin-bottom:6px;cursor:inherit}}
  .stat-tip{{opacity:0.55;font-size:0.82em;font-weight:400}}
  .stat-card .val{{font-size:1.6em;font-weight:700;color:#1a3a5c}}
  .section-title{{font-size:1.05em;font-weight:700;color:#1a3a5c;margin:24px 0 12px}}
  .toolbar{{display:flex;align-items:center;gap:16px;flex-wrap:wrap;margin-bottom:14px}}
  .search-box{{padding:8px 12px;border:1px solid #ccd;border-radius:6px;
               font-size:0.95em;min-width:220px;flex:1;max-width:360px}}
  .filter-group{{display:flex;align-items:center;gap:10px;flex-wrap:wrap}}
  .filter-group label{{display:flex;align-items:center;gap:5px;font-size:0.9em;
                       cursor:pointer;font-weight:500;color:#333}}
  .filter-group input[type=checkbox]{{width:16px;height:16px;cursor:pointer;accent-color:#1a3a5c}}
  .group-select{{padding:7px 10px;border:1px solid #ccd;border-radius:6px;
                 font-size:0.9em;background:#fff;cursor:pointer}}
  .bk-table{{display:none}}
  .bk-table.active{{display:block}}
  table{{width:100%;border-collapse:collapse;font-size:0.88em}}
  thead th{{background:#1a3a5c;color:#fff;padding:10px 12px;text-align:left;
            cursor:pointer;user-select:none;white-space:nowrap}}
  thead th:hover{{background:#254d7a}}
  thead th .sort-icon{{margin-left:5px;opacity:0.6;font-size:0.8em}}
  tbody tr:nth-child(even){{background:#f7f9fc}}
  tbody tr:hover{{background:#e8eef8}}
  td{{padding:8px 12px;border-bottom:1px solid #e8e8e8;vertical-align:top}}
  .empty-msg{{color:#888;font-style:italic;padding:16px 0}}
</style>
</head>
<body>
<h1>{safe_h1}</h1>
<div class="meta">{safe_judge} &nbsp;·&nbsp; Generated {today}</div>
{filters_html}

<div class="tabs">
  <button class="tab active" onclick="showTab('summary',this)">Summary</button>
  <button class="tab" onclick="showTab('breakdown',this)">Element Breakdown</button>
  <button class="tab" onclick="showTab('segments',this)">Segment Statistics</button>
  <button class="tab" onclick="showTab('elements',this)">Element Details</button>
  <button class="tab" onclick="showTab('pcs',this)">PCS Details</button>
</div>

<!-- SUMMARY -->
<div id="summary" class="panel active">
{instructions_html}
  <div class="section-title">PCS Statistics</div>
  <div class="stats-grid">
    <div class="stat-card stat-with-tip" tabindex="0"><p class="stat-tip-bubble" role="tooltip">{TOOLTIP_TOTAL_SCORES}</p><h3 class="stat-label">Total PCS Scores<span class="stat-tip" aria-hidden="true"> ⓘ</span></h3><div class="val">{report_stats['pcs_total_scores']}</div></div>
    <div class="stat-card stat-with-tip" tabindex="0"><p class="stat-tip-bubble" role="tooltip">{TOOLTIP_THROWOUT_RATE}</p><h3 class="stat-label">Throwout Rate<span class="stat-tip" aria-hidden="true"> ⓘ</span></h3><div class="val">{report_stats['pcs_throwout_rate']:.1f}%</div></div>
    <div class="stat-card stat-with-tip" tabindex="0"><p class="stat-tip-bubble" role="tooltip">{TOOLTIP_ANOMALY_RATE}</p><h3 class="stat-label">Anomaly Rate<span class="stat-tip" aria-hidden="true"> ⓘ</span></h3><div class="val">{report_stats['pcs_anomaly_rate']:.1f}%</div></div>
    <div class="stat-card stat-with-tip" tabindex="0"><p class="stat-tip-bubble" role="tooltip">{TOOLTIP_RULE_ERROR_RATE}</p><h3 class="stat-label">Rule Error Rate<span class="stat-tip" aria-hidden="true"> ⓘ</span></h3><div class="val">{report_stats['pcs_rule_error_rate']:.1f}%</div></div>
  </div>
  <div class="section-title">Element Statistics</div>
  <div class="stats-grid">
    <div class="stat-card stat-with-tip" tabindex="0"><p class="stat-tip-bubble" role="tooltip">{TOOLTIP_TOTAL_SCORES}</p><h3 class="stat-label">Total Element Scores<span class="stat-tip" aria-hidden="true"> ⓘ</span></h3><div class="val">{report_stats['element_total_scores']}</div></div>
    <div class="stat-card stat-with-tip" tabindex="0"><p class="stat-tip-bubble" role="tooltip">{TOOLTIP_THROWOUT_RATE}</p><h3 class="stat-label">Throwout Rate<span class="stat-tip" aria-hidden="true"> ⓘ</span></h3><div class="val">{report_stats['element_throwout_rate']:.1f}%</div></div>
    <div class="stat-card stat-with-tip" tabindex="0"><p class="stat-tip-bubble" role="tooltip">{TOOLTIP_ANOMALY_RATE}</p><h3 class="stat-label">Anomaly Rate<span class="stat-tip" aria-hidden="true"> ⓘ</span></h3><div class="val">{report_stats['element_anomaly_rate']:.1f}%</div></div>
    <div class="stat-card stat-with-tip" tabindex="0"><p class="stat-tip-bubble" role="tooltip">{TOOLTIP_RULE_ERROR_RATE}</p><h3 class="stat-label">Rule Error Rate<span class="stat-tip" aria-hidden="true"> ⓘ</span></h3><div class="val">{report_stats['element_rule_error_rate']:.1f}%</div></div>
  </div>
</div>

<!-- ELEMENT BREAKDOWN -->
<div id="breakdown" class="panel">
  <div class="toolbar">
    <input class="search-box" type="text" placeholder="Search breakdown..."
           oninput="filterTable(activeBkTable(),this.value)">
    <div style="display:flex;align-items:center;gap:8px">
      <label style="font-weight:600;font-size:0.9em">Group by:</label>
      <select class="group-select" onchange="switchBreakdown(this.value)">
        <option value="bk-type">Element Type</option>
        <option value="bk-year">Element Type + Year</option>
        <option value="bk-comp">Element Type + Competition</option>
      </select>
    </div>
  </div>
  <div class="bk-table active" id="bk-type">{_html_table('bk-type-tbl', bk_type_h, bk_type_r)}</div>
  <div class="bk-table" id="bk-year">{_html_table('bk-year-tbl', bk_year_h, bk_year_r)}</div>
  <div class="bk-table" id="bk-comp">{_html_table('bk-comp-tbl', bk_comp_h, bk_comp_r)}</div>
</div>

<!-- SEGMENT STATISTICS -->
<div id="segments" class="panel">
  <div class="toolbar">
    <input class="search-box" type="text" placeholder="Search segments..."
           oninput="filterTable('seg-table',this.value)">
  </div>
  {_html_table('seg-table', seg_headers, seg_rows)}
</div>

<!-- ELEMENT DETAILS -->
<div id="elements" class="panel">
  <div class="toolbar">
    <input class="search-box" type="text" id="elem-search" placeholder="Search element details..."
           oninput="applyIssueFilter('elem-table','elem-search','elem-chk')">
    <div class="filter-group">
      <label><input type="checkbox" id="elem-chk-to" checked onchange="applyIssueFilter('elem-table','elem-search','elem-chk')"> Thrown Out</label>
      <label><input type="checkbox" id="elem-chk-an" checked onchange="applyIssueFilter('elem-table','elem-search','elem-chk')"> Anomalies</label>
      <label><input type="checkbox" id="elem-chk-re" checked onchange="applyIssueFilter('elem-table','elem-search','elem-chk')"> Rule Errors</label>
    </div>
  </div>
  {_html_table('elem-table', elem_headers, elem_rows)}
</div>

<!-- PCS DETAILS -->
<div id="pcs" class="panel">
  <div class="toolbar">
    <input class="search-box" type="text" id="pcs-search" placeholder="Search PCS details..."
           oninput="applyIssueFilter('pcs-table','pcs-search','pcs-chk')">
    <div class="filter-group">
      <label><input type="checkbox" id="pcs-chk-to" checked onchange="applyIssueFilter('pcs-table','pcs-search','pcs-chk')"> Thrown Out</label>
      <label><input type="checkbox" id="pcs-chk-an" checked onchange="applyIssueFilter('pcs-table','pcs-search','pcs-chk')"> Anomalies</label>
      <label><input type="checkbox" id="pcs-chk-re" checked onchange="applyIssueFilter('pcs-table','pcs-search','pcs-chk')"> Rule Errors</label>
    </div>
  </div>
  {_html_table('pcs-table', pcs_headers, pcs_rows)}
</div>

<script>
var _currentBk='bk-type';
function activeBkTable(){{
  var id=_currentBk+'-tbl'; return id;
}}
function switchBreakdown(val){{
  document.querySelectorAll('.bk-table').forEach(function(d){{d.classList.remove('active');}});
  document.getElementById(val).classList.add('active');
  _currentBk=val;
}}
function showTab(id,btn){{
  document.querySelectorAll('.panel').forEach(p=>p.classList.remove('active'));
  document.querySelectorAll('.tab').forEach(t=>t.classList.remove('active'));
  document.getElementById(id).classList.add('active');
  btn.classList.add('active');
}}
function filterTable(tableId,q){{
  q=q.toLowerCase();
  var rows=document.getElementById(tableId).querySelectorAll('tbody tr');
  rows.forEach(function(r){{
    r.style.display=r.textContent.toLowerCase().includes(q)?'':'none';
  }});
}}
function applyIssueFilter(tableId,searchId,chkPrefix){{
  var q=document.getElementById(searchId).value.toLowerCase();
  var showTO=document.getElementById(chkPrefix+'-to').checked;
  var showAN=document.getElementById(chkPrefix+'-an').checked;
  var showRE=document.getElementById(chkPrefix+'-re').checked;
  var rows=document.getElementById(tableId).querySelectorAll('tbody tr');
  rows.forEach(function(r){{
    var cells=r.querySelectorAll('td');
    var issueCell=cells[cells.length-1]?cells[cells.length-1].textContent:'';
    var issueMatch=(showTO&&issueCell.includes('Thrown Out'))||
                   (showAN&&issueCell.includes('Anomaly'))||
                   (showRE&&issueCell.includes('Rule Error'));
    var textMatch=!q||r.textContent.toLowerCase().includes(q);
    r.style.display=(issueMatch&&textMatch)?'':'none';
  }});
}}
function sortTable(tableId,col){{
  var tbl=document.getElementById(tableId);
  var tbody=tbl.querySelector('tbody');
  var rows=Array.from(tbody.querySelectorAll('tr'));
  var asc=tbl.dataset.sortCol==col&&tbl.dataset.sortDir=='asc';
  rows.sort(function(a,b){{
    var av=a.cells[col]?a.cells[col].textContent.trim():'';
    var bv=b.cells[col]?b.cells[col].textContent.trim():'';
    var an=parseFloat(av),bn=parseFloat(bv);
    if(!isNaN(an)&&!isNaN(bn))return asc?bn-an:an-bn;
    return asc?bv.localeCompare(av):av.localeCompare(bv);
  }});
  rows.forEach(r=>tbody.appendChild(r));
  tbl.dataset.sortCol=col;
  tbl.dataset.sortDir=asc?'desc':'asc';
  tbl.querySelectorAll('thead th .sort-icon').forEach(function(ic,i){{
    ic.textContent=i==col?(asc?'▲':'▼'):'⇅';
  }});
}}
</script>
</body>
</html>"""
    return html.encode('utf-8')
