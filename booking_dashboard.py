"""
WIOM Booking Flow Funnel Dashboard
Install-cohort based: groups by install date so funnel is always correct
(installs >= homepage >= serviceable >= ... at every level).
Only first-time installers with 2026_ app version.
"""
import json, os, ssl, urllib.request
from datetime import datetime, timedelta

OUT = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'output_booking')
os.makedirs(OUT, exist_ok=True)

print("Reading API key...")
api_key = os.environ.get('METABASE_API_KEY')
if not api_key:
    env_path = r"C:\credentials\.env"
    if os.path.exists(env_path):
        with open(env_path, 'r') as f:
            for line in f:
                line = line.strip()
                if line.startswith('METABASE_API_KEY='):
                    api_key = line.split('=', 1)[1].strip().strip('"').strip("'")
                    break
if not api_key:
    raise RuntimeError("METABASE_API_KEY not found")

METABASE_URL = "https://metabase.wiom.in/api/dataset"
DB_ID = 113

def run_query(sql):
    payload = json.dumps({'database': DB_ID, 'type': 'native', 'native': {'query': sql}}).encode()
    ctx = ssl.create_default_context()
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_NONE
    req = urllib.request.Request(METABASE_URL, data=payload,
        headers={'Content-Type': 'application/json', 'X-Api-Key': api_key}, method='POST')
    resp = urllib.request.urlopen(req, context=ctx, timeout=600)
    data = json.loads(resp.read())
    return [c['name'] for c in data['data']['cols']], data['data']['rows']

INSTALL_BASE_CTE = """install_base AS (
  SELECT DISTINCT USER_ID, CAST(TIMESTAMP AS DATE) as install_date
  FROM PROD_DB.PUBLIC.CLEVERTAP_CUSTOMER
  WHERE EVENT_NAME = 'App Installed' AND TIMESTAMP >= '2026-01-26'
  AND TRY_PARSE_JSON(PROPERTIES):"event_props.ct_app_version"::STRING LIKE '2026_%'
  AND TRY_PARSE_JSON(PROPERTIES):"profile.events.App Installed.count"::INT = 1
)"""

DOWNSTREAM_EVENTS = """('booking_homepage_loaded','serviceable_page_loaded','unserviceable_page_loaded',
'how_does_it_work_clicked','how_to_get_started_clicked','cost_today_clicked',
'pay_100_to_move_forward_clicked','booking_fee_captured','choose_different_location_clicked')"""

# =====================================================================
# QUERY 1: Daily install cohort funnel
# =====================================================================
print("Fetching daily install cohort funnel...")
q1 = f"""WITH {INSTALL_BASE_CTE}
SELECT ib.install_date,
  COUNT(DISTINCT ib.USER_ID) as installs,
  COUNT(DISTINCT CASE WHEN c.EVENT_NAME='booking_homepage_loaded' THEN c.USER_ID END) as homepage,
  COUNT(DISTINCT CASE WHEN c.EVENT_NAME='serviceable_page_loaded' THEN c.USER_ID END) as serviceable,
  COUNT(DISTINCT CASE WHEN c.EVENT_NAME='unserviceable_page_loaded' THEN c.USER_ID END) as unserviceable,
  COUNT(DISTINCT CASE WHEN c.EVENT_NAME='how_does_it_work_clicked' THEN c.USER_ID END) as how_works,
  COUNT(DISTINCT CASE WHEN c.EVENT_NAME='how_to_get_started_clicked' THEN c.USER_ID END) as get_started,
  COUNT(DISTINCT CASE WHEN c.EVENT_NAME='cost_today_clicked' THEN c.USER_ID END) as cost_today,
  COUNT(DISTINCT CASE WHEN c.EVENT_NAME='pay_100_to_move_forward_clicked' THEN c.USER_ID END) as pay_100,
  COUNT(DISTINCT CASE WHEN c.EVENT_NAME='booking_fee_captured' THEN c.USER_ID END) as fee_captured,
  COUNT(DISTINCT CASE WHEN c.EVENT_NAME='choose_different_location_clicked' THEN c.USER_ID END) as diff_location
FROM install_base ib
LEFT JOIN PROD_DB.PUBLIC.CLEVERTAP_CUSTOMER c ON c.USER_ID = ib.USER_ID
  AND c.EVENT_NAME IN {DOWNSTREAM_EVENTS} AND c.TIMESTAMP >= '2026-01-26'
GROUP BY ib.install_date ORDER BY ib.install_date"""
_, cohort_rows = run_query(q1)

# Format: [{d, installs, homepage, serviceable, ...}, ...]
cohort_data = []
for r in cohort_rows:
    cohort_data.append({
        'd': r[0][:10], 'installs': r[1], 'homepage': r[2], 'serviceable': r[3],
        'unserviceable': r[4], 'how_works': r[5], 'get_started': r[6],
        'cost_today': r[7], 'pay_100': r[8], 'fee_captured': r[9], 'diff_location': r[10]
    })

# =====================================================================
# QUERY 2: Language by event (direct query, no heavy JOIN)
# =====================================================================
print("Fetching language breakdown...")
q2 = """SELECT EVENT_NAME,
  COALESCE(NULLIF(TRY_PARSE_JSON(PROPERTIES):"event_props.language"::STRING,''), 'unknown') as language,
  CAST(TIMESTAMP AS DATE) as event_date,
  COUNT(DISTINCT USER_ID) as unique_users
FROM PROD_DB.PUBLIC.CLEVERTAP_CUSTOMER
WHERE EVENT_NAME IN ('App Installed','booking_homepage_loaded','serviceable_page_loaded',
'unserviceable_page_loaded','how_does_it_work_clicked','how_to_get_started_clicked',
'cost_today_clicked','pay_100_to_move_forward_clicked','booking_fee_captured','choose_different_location_clicked')
AND TIMESTAMP >= '2026-01-26'
GROUP BY EVENT_NAME, language, event_date ORDER BY event_date, EVENT_NAME"""
_, lang_rows = run_query(q2)
lang_data = []
for r in lang_rows:
    lang = r[1] if r[1] in ('hi', 'en') else 'hi'
    lang_data.append({'e': r[0], 'l': lang, 'd': r[2][:10], 'u': r[3]})
# Merge duplicates after normalization
lang_merged = {}
for r in lang_data:
    key = (r['e'], r['l'], r['d'])
    lang_merged[key] = lang_merged.get(key, 0) + r['u']
lang_data = [{'e': k[0], 'l': k[1], 'd': k[2], 'u': v} for k, v in lang_merged.items()]

# For booking_fee_captured: override with pay_100 language (previous screen)
print("Fetching booking_fee language from pay_100...")
q2b = """SELECT
  COALESCE(NULLIF(TRY_PARSE_JSON(c2.PROPERTIES):"event_props.language"::STRING,''), 'hi') as lang,
  CAST(c1.TIMESTAMP AS DATE) as event_date,
  COUNT(DISTINCT c1.USER_ID) as users
FROM PROD_DB.PUBLIC.CLEVERTAP_CUSTOMER c1
INNER JOIN PROD_DB.PUBLIC.CLEVERTAP_CUSTOMER c2
  ON c1.USER_ID = c2.USER_ID
  AND c2.EVENT_NAME = 'pay_100_to_move_forward_clicked'
  AND c2.TIMESTAMP >= '2026-01-26'
WHERE c1.EVENT_NAME = 'booking_fee_captured' AND c1.TIMESTAMP >= '2026-01-26'
GROUP BY lang, event_date ORDER BY event_date"""
_, fee_lang_rows = run_query(q2b)
lang_data = [r for r in lang_data if r['e'] != 'booking_fee_captured']
for r in fee_lang_rows:
    lang = r[0] if r[0] in ('hi', 'en') else 'hi'
    lang_data.append({'e': 'booking_fee_captured', 'l': lang, 'd': r[1][:10], 'u': r[2]})

# =====================================================================
# QUERY 3: Location change page + language (direct query)
# =====================================================================
print("Fetching location change breakdown...")
q3 = """SELECT
  COALESCE(NULLIF(TRY_PARSE_JSON(PROPERTIES):"event_props.page_name"::STRING,''), 'unknown') as page,
  COALESCE(NULLIF(TRY_PARSE_JSON(PROPERTIES):"event_props.language"::STRING,''), 'hi') as language,
  CAST(TIMESTAMP AS DATE) as event_date,
  COUNT(DISTINCT USER_ID) as unique_users
FROM PROD_DB.PUBLIC.CLEVERTAP_CUSTOMER
WHERE EVENT_NAME = 'choose_different_location_clicked' AND TIMESTAMP >= '2026-01-26'
GROUP BY page, language, event_date ORDER BY event_date"""
_, loc_rows = run_query(q3)
loc_data = [{'p': r[0], 'l': r[1] if r[1] in ('hi','en') else 'hi', 'd': r[2][:10], 'u': r[3]} for r in loc_rows]

# =====================================================================
# QUERY 4: Language change behavior - on which screen users change
# =====================================================================
print("Fetching language change behavior...")
q4 = """SELECT
  COALESCE(NULLIF(TRY_PARSE_JSON(PROPERTIES):"event_props.page_name"::STRING,''), 'unknown') as page,
  COALESCE(NULLIF(TRY_PARSE_JSON(PROPERTIES):"event_props.language"::STRING,''), 'unknown') as to_lang,
  CAST(TIMESTAMP AS DATE) as event_date,
  COUNT(*) as total_changes,
  COUNT(DISTINCT USER_ID) as unique_users
FROM PROD_DB.PUBLIC.CLEVERTAP_CUSTOMER
WHERE EVENT_NAME = 'language_changed' AND TIMESTAMP >= '2026-01-26'
GROUP BY page, to_lang, event_date ORDER BY event_date"""
_, langchange_rows = run_query(q4)
langchange_data = [{'p': r[0], 'l': r[1], 'd': r[2][:10], 'c': r[3], 'u': r[4]} for r in langchange_rows]
# Filter out unknown and hamburger (not real pages)
langchange_data = [r for r in langchange_data if r['p'] != 'unknown' and 'hamburger' not in r['p'].lower()]

# =====================================================================
# QUERY 5: Distinct app versions
# =====================================================================
print("Fetching app versions...")
q5 = """SELECT DISTINCT TRY_PARSE_JSON(PROPERTIES):"event_props.ct_app_version"::STRING as app_version
FROM PROD_DB.PUBLIC.CLEVERTAP_CUSTOMER
WHERE EVENT_NAME = 'App Installed' AND TIMESTAMP >= '2026-01-26'
AND TRY_PARSE_JSON(PROPERTIES):"event_props.ct_app_version"::STRING LIKE '2026_%'
AND TRY_PARSE_JSON(PROPERTIES):"profile.events.App Installed.count"::INT = 1
ORDER BY app_version"""
_, av_rows = run_query(q5)
app_versions = sorted([r[0] for r in av_rows if r[0]])

# =====================================================================
# QUERY 6: Funnel by app version (aggregated totals per version)
# =====================================================================
print("Fetching version-wise funnel...")
q6 = f"""WITH install_base_v AS (
  SELECT DISTINCT USER_ID,
    TRY_PARSE_JSON(PROPERTIES):"event_props.ct_app_version"::STRING as app_version
  FROM PROD_DB.PUBLIC.CLEVERTAP_CUSTOMER
  WHERE EVENT_NAME = 'App Installed' AND TIMESTAMP >= '2026-01-26'
  AND TRY_PARSE_JSON(PROPERTIES):"event_props.ct_app_version"::STRING LIKE '2026_%'
  AND TRY_PARSE_JSON(PROPERTIES):"profile.events.App Installed.count"::INT = 1
)
SELECT ib.app_version,
  COUNT(DISTINCT ib.USER_ID) as installs,
  COUNT(DISTINCT CASE WHEN c.EVENT_NAME='booking_homepage_loaded' THEN c.USER_ID END) as homepage,
  COUNT(DISTINCT CASE WHEN c.EVENT_NAME='serviceable_page_loaded' THEN c.USER_ID END) as serviceable,
  COUNT(DISTINCT CASE WHEN c.EVENT_NAME='unserviceable_page_loaded' THEN c.USER_ID END) as unserviceable,
  COUNT(DISTINCT CASE WHEN c.EVENT_NAME='how_does_it_work_clicked' THEN c.USER_ID END) as how_works,
  COUNT(DISTINCT CASE WHEN c.EVENT_NAME='how_to_get_started_clicked' THEN c.USER_ID END) as get_started,
  COUNT(DISTINCT CASE WHEN c.EVENT_NAME='cost_today_clicked' THEN c.USER_ID END) as cost_today,
  COUNT(DISTINCT CASE WHEN c.EVENT_NAME='pay_100_to_move_forward_clicked' THEN c.USER_ID END) as pay_100,
  COUNT(DISTINCT CASE WHEN c.EVENT_NAME='booking_fee_captured' THEN c.USER_ID END) as fee_captured,
  COUNT(DISTINCT CASE WHEN c.EVENT_NAME='choose_different_location_clicked' THEN c.USER_ID END) as diff_location
FROM install_base_v ib
LEFT JOIN PROD_DB.PUBLIC.CLEVERTAP_CUSTOMER c ON c.USER_ID = ib.USER_ID
  AND c.EVENT_NAME IN {DOWNSTREAM_EVENTS} AND c.TIMESTAMP >= '2026-01-26'
GROUP BY ib.app_version ORDER BY ib.app_version"""
try:
    _, ver_rows = run_query(q6)
    version_data = []
    for r in ver_rows:
        version_data.append({
            'v': r[0], 'installs': r[1], 'homepage': r[2], 'serviceable': r[3],
            'unserviceable': r[4], 'how_works': r[5], 'get_started': r[6],
            'cost_today': r[7], 'pay_100': r[8], 'fee_captured': r[9], 'diff_location': r[10]
        })
except Exception as e:
    print(f"Warning: Version funnel query failed: {e}")
    version_data = []

# =====================================================================
# BUILD HTML
# =====================================================================
print("Building dashboard HTML...")
ist_now = datetime.utcnow() + timedelta(hours=5, minutes=30)
timestamp_str = ist_now.strftime('%d %b %Y, %I:%M %p IST')

all_dates = [r['d'] for r in cohort_data]
min_date = all_dates[0] if all_dates else '2026-01-26'
max_date = all_dates[-1] if all_dates else '2026-03-11'

html = """<!DOCTYPE html>
<html lang="en"><head><meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>WIOM Booking Flow Dashboard</title>
<script src="https://cdn.plot.ly/plotly-2.27.0.min.js"></script>
<style>
:root{--bg:#0f172a;--card:#1e293b;--border:#334155;--text:#e2e8f0;--muted:#94a3b8;--accent:#3b82f6;--green:#22c55e;--red:#ef4444;--orange:#f59e0b;--purple:#a855f7}
*{margin:0;padding:0;box-sizing:border-box}
body{font-family:'Segoe UI',system-ui,sans-serif;background:var(--bg);color:var(--text)}
.header{background:linear-gradient(135deg,#1e3a5f,#0f172a);padding:24px 32px;border-bottom:1px solid var(--border);display:flex;justify-content:space-between;align-items:center;flex-wrap:wrap;gap:12px}
.header h1{font-size:24px;font-weight:700} .header h1 span{color:var(--accent)}
.filter-bar{background:var(--card);border-bottom:1px solid var(--border);padding:14px 32px;display:flex;align-items:center;gap:12px;flex-wrap:wrap}
.filter-bar label{font-size:13px;color:var(--muted);font-weight:600}
.filter-bar input[type="date"]{background:var(--bg);color:var(--text);border:1px solid var(--border);border-radius:8px;padding:8px 12px;font-size:13px;cursor:pointer}
.filter-bar input[type="date"]::-webkit-calendar-picker-indicator{filter:invert(0.7);cursor:pointer}
.preset-btn{background:var(--bg);color:var(--muted);border:1px solid var(--border);border-radius:8px;padding:8px 16px;font-size:12px;cursor:pointer;transition:all 0.2s;font-weight:500}
.preset-btn:hover{border-color:var(--accent);color:var(--accent)}
.preset-btn.active{background:var(--accent);color:white;border-color:var(--accent)}
.sep{width:1px;height:28px;background:var(--border);margin:0 4px}
.tabs{display:flex;background:var(--card);border-bottom:2px solid var(--border);padding:0 24px;overflow-x:auto}
.tab{padding:14px 24px;cursor:pointer;font-size:14px;font-weight:500;color:var(--muted);border-bottom:3px solid transparent;transition:all 0.2s;white-space:nowrap}
.tab:hover{color:var(--text);background:rgba(59,130,246,0.1)}
.tab.active{color:var(--accent);border-bottom-color:var(--accent)}
.tc{display:none;padding:24px 32px} .tc.active{display:block}
.kg{display:grid;grid-template-columns:repeat(auto-fit,minmax(180px,1fr));gap:16px;margin-bottom:24px}
.kpi{background:var(--card);border:1px solid var(--border);border-radius:12px;padding:20px;text-align:center}
.kpi .v{font-size:28px;font-weight:700;color:var(--accent)} .kpi .l{font-size:12px;color:var(--muted);margin-top:4px}
.kpi.green .v{color:var(--green)} .kpi.red .v{color:var(--red)} .kpi.orange .v{color:var(--orange)} .kpi.purple .v{color:var(--purple)}
.cc{background:var(--card);border:1px solid var(--border);border-radius:12px;padding:20px;margin-bottom:20px}
.ct{font-size:16px;font-weight:600;margin-bottom:12px}
.g2{display:grid;grid-template-columns:1fr 1fr;gap:20px} @media(max-width:900px){.g2{grid-template-columns:1fr}}
table{width:100%;border-collapse:collapse;font-size:13px}
th{background:#0f172a;color:var(--accent);padding:10px 12px;text-align:left;font-weight:600;position:sticky;top:0}
td{padding:8px 12px;border-bottom:1px solid var(--border)} tr:hover{background:rgba(59,130,246,0.05)}
.ib{background:linear-gradient(135deg,rgba(59,130,246,0.1),rgba(168,85,247,0.1));border:1px solid rgba(59,130,246,0.3);border-radius:12px;padding:20px;margin-bottom:20px}
.ib h3{color:var(--accent);margin-bottom:10px;font-size:15px}
.ib p,.ib li{color:var(--muted);font-size:13px;line-height:1.7} .ib ul{padding-left:20px}
.refresh-btn{background:linear-gradient(135deg,#22c55e,#16a34a);color:white;border:none;border-radius:8px;padding:10px 20px;font-size:13px;font-weight:600;cursor:pointer;transition:all 0.2s;display:flex;align-items:center;gap:6px}
.refresh-btn:hover{transform:scale(1.05);box-shadow:0 4px 12px rgba(34,197,94,0.4)}
.refresh-btn:disabled{opacity:0.5;cursor:not-allowed;transform:none}
.refresh-status{color:var(--orange);font-size:12px;margin-top:4px;min-height:16px}
.cmp-select{background:var(--bg);color:var(--text);border:1px solid var(--border);border-radius:8px;padding:8px 12px;font-size:13px}
.cmp-btn{background:var(--accent);color:white;border:none;border-radius:8px;padding:10px 24px;font-size:13px;font-weight:600;cursor:pointer}
.cmp-btn:hover{background:#2563eb}
.cmp-label{font-size:13px;color:var(--muted);font-weight:600;margin-bottom:6px}
.overlay{display:none;position:fixed;top:0;left:0;right:0;bottom:0;background:rgba(0,0,0,0.7);z-index:9999;justify-content:center;align-items:center}
.overlay.show{display:flex}
.overlay-box{background:var(--card);border:1px solid var(--border);border-radius:16px;padding:40px;text-align:center}
.overlay-box .spinner{width:40px;height:40px;border:4px solid var(--border);border-top:4px solid var(--accent);border-radius:50%;animation:spin 1s linear infinite;margin:0 auto 16px}
@keyframes spin{to{transform:rotate(360deg)}}
</style></head><body>
<div id="refreshOverlay" class="overlay"><div class="overlay-box"><div class="spinner"></div><div id="refreshMsg" style="color:#e2e8f0;font-size:15px">Refreshing data...</div><div id="refreshDetail" style="color:#94a3b8;font-size:12px;margin-top:8px"></div></div></div>
<div class="header"><div>
<h1>WIOM <span>Booking Flow</span> Dashboard</h1>
<div style="color:#94a3b8;font-size:13px;margin-top:4px">First-time Installers Only (App Version 2026_*) | Install-Cohort Based Funnel</div>
</div><div style="text-align:right">
<button class="refresh-btn" id="refreshBtn" onclick="refreshAll()">&#x21bb; Refresh Data</button>
<div style="color:#94a3b8;font-size:12px;margin-top:4px">Last updated: <span id="lastUpdated">""" + timestamp_str + """</span></div>
</div></div>

<div class="filter-bar">
<label>Install Date Range:</label>
<input type="date" id="df" value=\"""" + min_date + """\" min=\"""" + min_date + """\" max=\"""" + max_date + """\">
<span style="color:#94a3b8">to</span>
<input type="date" id="dt" value=\"""" + max_date + """\" min=\"""" + min_date + """\" max=\"""" + max_date + """\">
<div class="sep"></div>
<button class="preset-btn" onclick="sp('7d',this)">Last 7 Days</button>
<button class="preset-btn" onclick="sp('14d',this)">Last 14 Days</button>
<button class="preset-btn" onclick="sp('30d',this)">Last 30 Days</button>
<button class="preset-btn" onclick="sp('tm',this)">This Month</button>
<button class="preset-btn" onclick="sp('lm',this)">Last Month</button>
<button class="preset-btn active" id="allBtn" onclick="sp('all',this)">All Time</button>
<div class="sep"></div>
<span id="dri" style="color:#94a3b8;font-size:12px"></span>
</div>

<div class="tabs">
<div class="tab active" onclick="st(0)">Funnel Overview</div>
<div class="tab" onclick="st(1)">Drop-off Analysis</div>
<div class="tab" onclick="st(2)">Weekly Trends</div>
<div class="tab" onclick="st(3)">Daily Trends</div>
<div class="tab" onclick="st(4)">Month-wise</div>
<div class="tab" onclick="st(5)">Serviceability</div>
<div class="tab" onclick="st(6)">Language Analysis</div>
<div class="tab" onclick="st(7)">Location Changes</div>
<div class="tab" onclick="st(8)">Language Change Behavior</div>
<div class="tab" onclick="st(9)">Funnel Comparison</div>
</div>

<div class="tc active" id="t0">
<div class="ib"><h3>What does this show?</h3><p>Booking funnel for <strong>first-time installers</strong> (App Installed count=1, version 2026_*). Data is grouped by <strong>install date</strong> so App Installed is always 100% — no downstream step can exceed it. Use the date filter to select which install cohorts to include.</p></div>
<div class="kg" id="k0"></div>
<div class="cc"><div class="ct">Booking Funnel</div><div id="c_funnel" style="height:500px"></div></div>
<div class="cc"><div class="ct">All Events Summary (Unique Users)</div><div style="overflow-x:auto"><table id="tb_summary"><thead><tr><th>Event</th><th>Unique Users</th><th>% of Installs</th></tr></thead><tbody></tbody></table></div></div>
</div>

<div class="tc" id="t1">
<div class="ib"><h3>What does this show?</h3><ul><li><strong>Drop-off</strong> = users from previous step who did NOT reach next step</li><li><strong>Conversion</strong> = % who progressed. Red &lt;30%, Orange 30-60%, Green &gt;60%</li></ul></div>
<div class="cc"><div class="ct">Step-by-Step Drop-off %</div><div id="c_dropoff" style="height:400px"></div></div>
<div class="cc"><div class="ct">Conversion Between Steps (Unique Users)</div><div style="overflow-x:auto"><table id="tb_dropoff"><thead><tr><th>From</th><th>To</th><th>Dropped (Users)</th><th>Drop %</th><th>Conv %</th></tr></thead><tbody></tbody></table></div></div>
<div class="g2">
<div class="cc"><div class="ct">Conversion Rates (%)</div><div id="c_conv" style="height:350px"></div></div>
<div class="cc"><div class="ct">Unique Users at Each Step</div><div id="c_step" style="height:350px"></div></div>
</div></div>

<div class="tc" id="t2">
<div class="ib"><h3>What does this show?</h3><p>Weekly install cohort trends. Each week shows users who installed that week and how many completed each funnel step.</p></div>
<div class="cc"><div class="ct">Weekly Install Cohort Funnel (Unique Users)</div><div id="c_weekly" style="height:450px"></div></div>
<div class="g2">
<div class="cc"><div class="ct">Weekly Install to Booking %</div><div id="c_wconv" style="height:350px"></div></div>
<div class="cc"><div class="ct">Weekly Serviceable vs Unserviceable</div><div id="c_wserv" style="height:350px"></div></div>
</div></div>

<div class="tc" id="t3">
<div class="ib"><h3>What does this show?</h3><p>Daily install cohort data. Each day shows installs and funnel completion for that day's installers.</p></div>
<div class="cc"><div class="ct">Daily Install Cohort Trends (Unique Users)</div><div id="c_daily" style="height:450px"></div></div>
</div>

<div class="tc" id="t4">
<div class="ib"><h3>What does this show?</h3><p>Month-wise breakdown with conversion rates.</p></div>
<div class="cc"><div class="ct">Month-wise Funnel</div><div style="overflow-x:auto"><table id="tb_month"><thead><tr><th>Month</th><th>Installs</th><th>Homepage</th><th>Serviceable</th><th>How Works</th><th>Cost Today</th><th>Pay 100</th><th>Booking</th><th>Home/Inst %</th><th>Book/Inst %</th><th>Unserv</th><th>Diff Loc</th></tr></thead><tbody></tbody></table></div></div>
<div class="cc"><div class="ct">Month-wise Comparison</div><div id="c_month" style="height:400px"></div></div>
</div>

<div class="tc" id="t5">
<div class="ib"><h3>What does this show?</h3><ul><li><strong>Serviceable</strong> = location covered by WIOM</li><li><strong>Unserviceable</strong> = NOT covered</li><li><strong>Choose Different Location</strong> = user changed location</li></ul></div>
<div class="kg" id="k5"></div>
<div class="g2">
<div class="cc"><div class="ct">Serviceable vs Unserviceable</div><div id="c_pie" style="height:350px"></div></div>
<div class="cc"><div class="ct">Location Actions</div><div id="c_locbar" style="height:350px"></div></div>
</div>
<div class="cc"><div class="ct">Weekly Trend</div><div id="c_strd" style="height:350px"></div></div>
</div>

<div class="tc" id="t6">
<div class="ib"><h3>What does this show?</h3><ul><li>Which <strong>language</strong> users are on at each funnel step</li><li>Users can change language on any page — this captures language at event time</li><li>Compare Hindi vs English funnel conversion rates</li></ul></div>
<div class="cc"><div class="ct">Hindi vs English at Each Step (Unique Users)</div><div id="c_lang" style="height:450px"></div></div>
<div class="cc"><div class="ct">Language Breakdown (Unique Users)</div><div style="overflow-x:auto"><table id="tb_lang"><thead><tr><th>Event</th><th>Total Users</th><th>Hindi Users</th><th>Hindi %</th><th>English Users</th><th>English %</th></tr></thead><tbody></tbody></table></div></div>
<div class="cc"><div class="ct">Hindi vs English Conversion % (Install = 100%)</div><div id="c_lconv" style="height:400px"></div></div>
</div>

<div class="tc" id="t7">
<div class="ib"><h3>What does this show?</h3><ul><li>On which <strong>page</strong> users click "Choose Different Location"</li><li>What <strong>language</strong> they use when changing location</li></ul></div>
<div class="kg" id="k7"></div>
<div class="cc"><div class="ct">Location Change by Page + Language (Unique Users)</div><div id="c_loc" style="height:400px"></div></div>
<div class="cc"><div class="ct">Detail Table</div><div style="overflow-x:auto"><table id="tb_loc"><thead><tr><th>Page</th><th>Language</th><th>Unique Users</th></tr></thead><tbody></tbody></table></div></div>
</div>

<div class="tc" id="t8">
<div class="ib"><h3>What does this show?</h3><ul><li>On which <strong>screen/page</strong> users change their language most often</li><li>Whether users switch <strong>to Hindi</strong> or <strong>to English</strong></li><li>Helps understand language preference behavior across the booking flow</li></ul></div>
<div class="kg" id="k8"></div>
<div class="g2">
<div class="cc"><div class="ct">Language Changes by Page - Top 10 (Unique Users)</div><div id="c_lcpage" style="height:400px"></div></div>
<div class="cc"><div class="ct">Switch To Hindi vs English by Page (Unique Users)</div><div id="c_lclang" style="height:400px"></div></div>
</div>
<div class="cc"><div class="ct">Language Change Detail</div><div style="overflow-x:auto"><table id="tb_lc"><thead><tr><th>Page</th><th>Switched To</th><th>Total Events</th><th>Unique Users</th></tr></thead><tbody></tbody></table></div></div>
</div>

<div class="tc" id="t9">
<div class="ib"><h3>What does this show?</h3><ul><li>Compare <strong>two different date ranges</strong> side-by-side to see funnel changes over time</li><li>Compare <strong>two app versions</strong> to see which version performs better</li><li>Green = improvement, Red = decline</li></ul></div>

<div class="cc">
<div class="ct">Date Range Comparison</div>
<div style="display:flex;gap:24px;flex-wrap:wrap;align-items:end;margin-bottom:16px">
<div><div class="cmp-label">Range A</div><input type="date" id="cdf1" class="cmp-select"> <span style="color:#94a3b8">to</span> <input type="date" id="cdt1" class="cmp-select"></div>
<div><div class="cmp-label">Range B</div><input type="date" id="cdf2" class="cmp-select"> <span style="color:#94a3b8">to</span> <input type="date" id="cdt2" class="cmp-select"></div>
<button class="cmp-btn" onclick="cmpDates()">Compare Dates</button>
</div></div>
<div class="g2">
<div class="cc"><div class="ct" id="cmpATitle">Range A Funnel</div><div id="c_cmpA" style="height:400px"></div></div>
<div class="cc"><div class="ct" id="cmpBTitle">Range B Funnel</div><div id="c_cmpB" style="height:400px"></div></div>
</div>
<div class="cc"><div class="ct">Comparison Table</div><div style="overflow-x:auto"><table id="tb_cmp"><thead><tr><th>Step</th><th>Range A Users</th><th>Range A %</th><th>Range B Users</th><th>Range B %</th><th>Diff %</th></tr></thead><tbody></tbody></table></div></div>

<div class="cc" style="margin-top:24px">
<div class="ct">App Version Comparison</div>
<div style="display:flex;gap:24px;flex-wrap:wrap;align-items:end;margin-bottom:16px">
<div><div class="cmp-label">Version A</div><select id="cv1" class="cmp-select"></select></div>
<div><div class="cmp-label">Version B</div><select id="cv2" class="cmp-select"></select></div>
<button class="cmp-btn" onclick="cmpVersions()">Compare Versions</button>
</div></div>
<div class="g2">
<div class="cc"><div class="ct" id="cmpVATitle">Version A Funnel</div><div id="c_cmpVA" style="height:400px"></div></div>
<div class="cc"><div class="ct" id="cmpVBTitle">Version B Funnel</div><div id="c_cmpVB" style="height:400px"></div></div>
</div>
<div class="cc"><div class="ct">Version Comparison Table</div><div style="overflow-x:auto"><table id="tb_vcmp"><thead><tr><th>Step</th><th>Ver A Users</th><th>Ver A %</th><th>Ver B Users</th><th>Ver B %</th><th>Diff %</th></tr></thead><tbody></tbody></table></div></div>
</div>

<script>
var CD = """ + json.dumps(cohort_data) + """;
var LD = """ + json.dumps(lang_data) + """;
var LOC = """ + json.dumps(loc_data) + """;
var LC = """ + json.dumps(langchange_data) + """;
var AV = """ + json.dumps(app_versions) + """;
var VD = """ + json.dumps(version_data) + """;
var MIND='""" + min_date + """', MAXD='""" + max_date + """';

var FK=['installs','homepage','serviceable','how_works','cost_today','pay_100','fee_captured'];
var FL=['App Installed','Homepage Loaded','Serviceable Page','How Does It Work','Cost Today','Pay 100 Clicked','Booking Captured'];
var FC=['#3b82f6','#6366f1','#8b5cf6','#a855f7','#f59e0b','#ef4444','#22c55e'];
var AK=['installs','homepage','serviceable','unserviceable','how_works','get_started','cost_today','pay_100','fee_captured','diff_location'];
var AL=['App Installed','Homepage Loaded','Serviceable Page','Unserviceable Page','How Does It Work','How to Get Started','Cost Today','Pay 100 Clicked','Booking Captured','Diff Location'];
var EK={'App Installed':'installs','booking_homepage_loaded':'homepage','serviceable_page_loaded':'serviceable',
'unserviceable_page_loaded':'unserviceable','how_does_it_work_clicked':'how_works','how_to_get_started_clicked':'get_started',
'cost_today_clicked':'cost_today','pay_100_to_move_forward_clicked':'pay_100','booking_fee_captured':'fee_captured',
'choose_different_location_clicked':'diff_location'};

var RC={responsive:true};
function L(o){var b={paper_bgcolor:'#1e293b',plot_bgcolor:'#1e293b',font:{color:'#94a3b8',size:12},xaxis:{gridcolor:'#334155'},yaxis:{gridcolor:'#334155'},margin:{t:30,b:50,l:60,r:20},legend:{orientation:'h',y:-0.15}};if(o)for(var k in o)b[k]=o[k];return b}
function fmt(n){return n.toLocaleString()}

var at=0;
function dr(){return{f:document.getElementById('df').value,t:document.getElementById('dt').value}}
function filt(){var d=dr();return CD.filter(function(r){return r.d>=d.f&&r.d<=d.t})}
function filtArr(arr){var d=dr();return arr.filter(function(r){return r.d>=d.f&&r.d<=d.t})}
function agg(fd){var s={};AK.forEach(function(k){s[k]=0});fd.forEach(function(r){AK.forEach(function(k){s[k]+=r[k]})});return s}
function byWeek(fd){var m={};fd.forEach(function(r){var d=new Date(r.d),dy=d.getDay(),df=d.getDate()-dy+(dy===0?-6:1),mon=new Date(d.setDate(df)),wk=mon.toISOString().slice(0,10);if(!m[wk])m[wk]={};AK.forEach(function(k){m[wk][k]=(m[wk][k]||0)+r[k]})});return m}
function byMonth(fd){var m={};fd.forEach(function(r){var mo=r.d.slice(0,7);if(!m[mo])m[mo]={};AK.forEach(function(k){m[mo][k]=(m[mo][k]||0)+r[k]})});return m}

function sp(p,el){var td=new Date(MAXD),f,t=MAXD;
if(p==='7d'){var d=new Date(td);d.setDate(d.getDate()-6);f=d.toISOString().slice(0,10)}
else if(p==='14d'){var d=new Date(td);d.setDate(d.getDate()-13);f=d.toISOString().slice(0,10)}
else if(p==='30d'){var d=new Date(td);d.setDate(d.getDate()-29);f=d.toISOString().slice(0,10)}
else if(p==='tm'){f=MAXD.slice(0,7)+'-01'}
else if(p==='lm'){var d=new Date(td.getFullYear(),td.getMonth()-1,1);f=d.toISOString().slice(0,10);var d2=new Date(td.getFullYear(),td.getMonth(),0);t=d2.toISOString().slice(0,10)}
else{f=MIND;t=MAXD}
if(f<MIND)f=MIND;document.getElementById('df').value=f;document.getElementById('dt').value=t;
document.querySelectorAll('.preset-btn').forEach(function(b){b.classList.remove('active')});el.classList.add('active');render()}
document.getElementById('df').addEventListener('change',function(){document.querySelectorAll('.preset-btn').forEach(function(b){b.classList.remove('active')});render()});
document.getElementById('dt').addEventListener('change',function(){document.querySelectorAll('.preset-btn').forEach(function(b){b.classList.remove('active')});render()});

function st(n){at=n;document.querySelectorAll('.tab').forEach(function(t,i){t.classList.toggle('active',i===n)});
document.querySelectorAll('.tc').forEach(function(t,i){t.classList.toggle('active',i===n)});render()}

function render(){
var fd=filt(),s=agg(fd);
var days=Math.round((new Date(document.getElementById('dt').value)-new Date(document.getElementById('df').value))/86400000)+1;
document.getElementById('dri').textContent=days+' days | '+fmt(s.installs)+' installs';
window['rt'+at](fd,s);
}

window.rt0=function(fd,s){
var vals=FK.map(function(k){return s[k]});
var inst=s.installs||1,book=s.fee_captured,conv=Math.round(book/inst*10000)/100;
var hpU=s.homepage||1,sR=Math.round(s.serviceable/hpU*1000)/10,uR=Math.round(s.unserviceable/hpU*1000)/10;
document.getElementById('k0').innerHTML=
'<div class="kpi"><div class="v">'+fmt(s.installs)+'</div><div class="l">First-time Installs (Unique Users)</div></div>'+
'<div class="kpi green"><div class="v">'+fmt(book)+'</div><div class="l">Bookings (Unique Users)</div></div>'+
'<div class="kpi purple"><div class="v">'+conv+'%</div><div class="l">Install to Booking %</div></div>'+
'<div class="kpi orange"><div class="v">'+sR+'%</div><div class="l">Serviceable Rate %</div></div>'+
'<div class="kpi red"><div class="v">'+uR+'%</div><div class="l">Unserviceable Rate %</div></div>';
Plotly.newPlot('c_funnel',[{type:'funnel',y:FL,x:vals,textinfo:'value+percent initial',marker:{color:FC},connector:{line:{color:'#334155'}}}],L({margin:{t:20,b:20,l:180,r:80},showlegend:false}),RC);
var tb='';AK.forEach(function(k,i){var u=s[k],pct=Math.round(u/inst*1000)/10;tb+='<tr><td>'+AL[i]+'</td><td>'+fmt(u)+'</td><td style="color:var(--accent);font-weight:600">'+pct+'%</td></tr>'});
document.querySelector('#tb_summary tbody').innerHTML=tb;
};

window.rt1=function(fd,s){
var vals=FK.map(function(k){return s[k]});
var lbl=[],cv=[],dr=[],cl=[],tb='';
for(var i=1;i<vals.length;i++){var p=vals[i-1]||1,c=vals[i],d=p-c,dp=Math.round(d/p*1000)/10,co=Math.round(c/p*1000)/10;
var lb=FL[i-1]+' -> '+FL[i];lbl.push(lb);cv.push(co);dr.push(dp);
var clr=co>60?'#22c55e':co>30?'#f59e0b':'#ef4444';cl.push(clr);
tb+='<tr><td>'+FL[i-1]+'</td><td>'+FL[i]+'</td><td>'+fmt(d)+'</td><td style="color:#ef4444">'+dp+'%</td><td style="color:'+clr+';font-weight:600">'+co+'%</td></tr>'}
document.querySelector('#tb_dropoff tbody').innerHTML=tb;
Plotly.newPlot('c_dropoff',[{x:lbl,y:dr,type:'bar',marker:{color:'#ef4444'},text:dr.map(function(v){return v+'%'}),textposition:'outside'}],L({yaxis:{gridcolor:'#334155',title:'Drop %'}}),RC);
Plotly.newPlot('c_conv',[{x:lbl,y:cv,type:'bar',marker:{color:cl},text:cv.map(function(v){return v+'%'}),textposition:'outside'}],L({yaxis:{gridcolor:'#334155',title:'Conv %'},showlegend:false}),RC);
var inst=vals[0]||1;Plotly.newPlot('c_step',[{x:FL,y:vals,type:'bar',marker:{color:FC},text:vals.map(function(v){return fmt(v)+' ('+Math.round(v/inst*1000)/10+'%)'}),textposition:'outside'}],L({showlegend:false,yaxis:{gridcolor:'#334155',title:'Unique Users'}}),RC);
};

window.rt2=function(fd){
var wk=byWeek(fd),weeks=Object.keys(wk).sort();
var tr=FK.map(function(k,i){return{x:weeks,y:weeks.map(function(w){return wk[w][k]||0}),name:FL[i],type:'scatter',mode:'lines+markers',text:weeks.map(function(w){var v=wk[w][k]||0,inst=wk[w].installs||1;return fmt(v)+' ('+Math.round(v/inst*1000)/10+'%)'}),hovertemplate:'%{text}'}});
Plotly.newPlot('c_weekly',tr,L({xaxis:{gridcolor:'#334155',title:'Week'},yaxis:{gridcolor:'#334155',title:'Unique Users'}}),RC);
var cd=weeks.map(function(w){var inst=wk[w].installs||1;return Math.round((wk[w].fee_captured||0)/inst*10000)/100});
Plotly.newPlot('c_wconv',[{x:weeks,y:cd,type:'scatter',mode:'lines+markers+text',text:cd.map(function(v){return v+'%'}),textposition:'top',line:{color:'#22c55e',width:3},marker:{size:8}}],L({showlegend:false,yaxis:{gridcolor:'#334155',title:'Conv %'}}),RC);
Plotly.newPlot('c_wserv',[{x:weeks,y:weeks.map(function(w){return wk[w].serviceable||0}),name:'Serviceable',type:'bar',marker:{color:'#22c55e'},text:weeks.map(function(w){var v=wk[w].serviceable||0,hp=wk[w].homepage||1;return fmt(v)+' ('+Math.round(v/hp*1000)/10+'%)'}),textposition:'outside'},{x:weeks,y:weeks.map(function(w){return wk[w].unserviceable||0}),name:'Unserviceable',type:'bar',marker:{color:'#ef4444'},text:weeks.map(function(w){var v=wk[w].unserviceable||0,hp=wk[w].homepage||1;return fmt(v)+' ('+Math.round(v/hp*1000)/10+'%)'}),textposition:'outside'}],L({barmode:'group',yaxis:{gridcolor:'#334155',title:'Unique Users'}}),RC);
};

window.rt3=function(fd){
var days=fd.map(function(r){return r.d});
var tr=FK.map(function(k,i){return{x:days,y:fd.map(function(r){return r[k]}),name:FL[i],type:'scatter',mode:'lines+markers',text:fd.map(function(r){var v=r[k],inst=r.installs||1;return fmt(v)+' ('+Math.round(v/inst*1000)/10+'%)'}),hovertemplate:'%{text}'}});
Plotly.newPlot('c_daily',tr,L({xaxis:{gridcolor:'#334155',title:'Install Date'},yaxis:{gridcolor:'#334155',title:'Unique Users'}}),RC);
};

window.rt4=function(fd){
var mo=byMonth(fd),months=Object.keys(mo).sort(),rmo=months.slice().reverse(),tb='';
rmo.forEach(function(m){var d=mo[m],vals=FK.map(function(k){return d[k]||0});
var hpP=vals[0]>0?Math.round(vals[1]/vals[0]*1000)/10:0;
var bkP=vals[0]>0?Math.round(vals[6]/vals[0]*1000)/10:0;
tb+='<tr><td style="font-weight:600">'+m+'</td>';vals.forEach(function(v){tb+='<td>'+fmt(v)+'</td>'});
tb+='<td style="color:#2196F3;font-weight:600">'+hpP+'%</td><td style="color:#4CAF50;font-weight:600">'+bkP+'%</td>';
tb+='<td>'+fmt(d.unserviceable||0)+'</td><td>'+fmt(d.diff_location||0)+'</td></tr>'});
document.querySelector('#tb_month tbody').innerHTML=tb;
var tr=FK.map(function(k,i){return{x:months,y:months.map(function(m){return mo[m][k]||0}),name:FL[i],type:'bar',marker:{color:FC[i]}}});
Plotly.newPlot('c_month',tr,L({barmode:'group'}),RC);
};

window.rt5=function(fd,s){
document.getElementById('k5').innerHTML=
'<div class="kpi green"><div class="v">'+fmt(s.serviceable)+'</div><div class="l">Serviceable (Unique Users)</div></div>'+
'<div class="kpi red"><div class="v">'+fmt(s.unserviceable)+'</div><div class="l">Unserviceable (Unique Users)</div></div>'+
'<div class="kpi orange"><div class="v">'+fmt(s.diff_location)+'</div><div class="l">Changed Location (Unique Users)</div></div>'+
'<div class="kpi purple"><div class="v">'+fmt(s.get_started)+'</div><div class="l">How to Get Started (Unique Users)</div></div>';
Plotly.newPlot('c_pie',[{values:[s.serviceable,s.unserviceable],labels:['Serviceable','Unserviceable'],type:'pie',hole:0.5,marker:{colors:['#22c55e','#ef4444']},textinfo:'label+percent+value'}],L({showlegend:false}),RC);
var hpT=s.homepage||1;Plotly.newPlot('c_locbar',[{x:['Serviceable','Unserviceable','Changed Loc','Get Started'],y:[s.serviceable,s.unserviceable,s.diff_location,s.get_started],type:'bar',marker:{color:['#22c55e','#ef4444','#f59e0b','#a855f7']},text:[s.serviceable,s.unserviceable,s.diff_location,s.get_started].map(function(v){return fmt(v)+' ('+Math.round(v/hpT*1000)/10+'%)'}),textposition:'outside'}],L({showlegend:false,yaxis:{gridcolor:'#334155',title:'Unique Users'}}),RC);
var wk=byWeek(fd),weeks=Object.keys(wk).sort();
Plotly.newPlot('c_strd',[{x:weeks,y:weeks.map(function(w){return wk[w].serviceable||0}),name:'Serviceable',type:'scatter',mode:'lines+markers',line:{color:'#22c55e'}},{x:weeks,y:weeks.map(function(w){return wk[w].unserviceable||0}),name:'Unserviceable',type:'scatter',mode:'lines+markers',line:{color:'#ef4444'}}],L(),RC);
};

window.rt6=function(){
var fLD=filtArr(LD);
var le={};fLD.forEach(function(r){if(!le[r.e])le[r.e]={};le[r.e][r.l]=(le[r.e][r.l]||0)+r.u});
var evts=['App Installed','booking_homepage_loaded','serviceable_page_loaded','unserviceable_page_loaded','how_does_it_work_clicked','how_to_get_started_clicked','cost_today_clicked','pay_100_to_move_forward_clicked','booking_fee_captured','choose_different_location_clicked'];
var labels=['App Installed','Homepage Loaded','Serviceable','Unserviceable','How It Works','Get Started','Cost Today','Pay 100','Booking','Diff Location'];
var funnelEvts=['App Installed','booking_homepage_loaded','serviceable_page_loaded','how_does_it_work_clicked','cost_today_clicked','pay_100_to_move_forward_clicked','booking_fee_captured'];
var tb='',hiF=[],enF=[];
evts.forEach(function(e,i){var lg=le[e]||{},hi=lg['hi']||0,en=lg['en']||0,tot=hi+en;
var hP=tot>0?Math.round(hi/tot*1000)/10:0,eP=tot>0?Math.round(en/tot*1000)/10:0;
tb+='<tr><td style="font-weight:600">'+labels[i]+'</td><td>'+fmt(tot)+'</td><td>'+fmt(hi)+'</td><td style="color:#f59e0b">'+hP+'%</td><td>'+fmt(en)+'</td><td style="color:#3b82f6">'+eP+'%</td></tr>'});
funnelEvts.forEach(function(e){hiF.push((le[e]||{})['hi']||0);enF.push((le[e]||{})['en']||0)});
document.querySelector('#tb_lang tbody').innerHTML=tb;
var hiT=hiF.reduce(function(a,b){return a+b},0)||1,enT=enF.reduce(function(a,b){return a+b},0)||1;
Plotly.newPlot('c_lang',[{x:FL,y:hiF,name:'Hindi',type:'bar',marker:{color:'#f59e0b'},text:hiF.map(function(v){var inst=hiF[0]||1;return fmt(v)+' ('+Math.round(v/inst*1000)/10+'%)'}),textposition:'outside'},{x:FL,y:enF,name:'English',type:'bar',marker:{color:'#3b82f6'},text:enF.map(function(v){var inst=enF[0]||1;return fmt(v)+' ('+Math.round(v/inst*1000)/10+'%)'}),textposition:'outside'}],L({barmode:'group',yaxis:{gridcolor:'#334155',title:'Unique Users'}}),RC);
var hB=hiF[0]||1,eB=enF[0]||1;
Plotly.newPlot('c_lconv',[{x:FL,y:hiF.map(function(v,i){return i===0?100:Math.round(v/hB*1000)/10}),name:'Hindi %',type:'scatter',mode:'lines+markers',line:{color:'#f59e0b',width:3}},{x:FL,y:enF.map(function(v,i){return i===0?100:Math.round(v/eB*1000)/10}),name:'English %',type:'scatter',mode:'lines+markers',line:{color:'#3b82f6',width:3}}],L({yaxis:{gridcolor:'#334155',title:'% of Install Base'}}),RC);
};

window.rt7=function(){
var fLOC=filtArr(LOC);
// Aggregate filtered data by page+lang
var lm={};fLOC.forEach(function(r){var k=r.p+'|'+r.l;lm[k]=(lm[k]||0)+r.u});
var aLOC=Object.keys(lm).map(function(k){var p=k.split('|');return{p:p[0],l:p[1],u:lm[k]}}).sort(function(a,b){return b.u-a.u});
var t3=aLOC.slice(0,3),total=0;aLOC.forEach(function(r){total+=r.u});
var kh='<div class="kpi orange"><div class="v">'+fmt(total)+'</div><div class="l">Total Location Changes</div></div>';
t3.forEach(function(r){var ln=r.l==='hi'?'Hindi':r.l==='en'?'English':r.l;kh+='<div class="kpi"><div class="v">'+fmt(r.u)+'</div><div class="l">'+r.p+' ('+ln+')</div></div>'});
document.getElementById('k7').innerHTML=kh;
var t10=aLOC.slice(0,10),px=t10.map(function(r){var ln=r.l==='hi'?'Hindi':r.l==='en'?'English':r.l;return r.p+' ('+ln+')'}),uy=t10.map(function(r){return r.u}),cs=t10.map(function(r){return r.l==='hi'?'#f59e0b':r.l==='en'?'#3b82f6':'#94a3b8'});
Plotly.newPlot('c_loc',[{x:px,y:uy,type:'bar',marker:{color:cs},text:uy.map(function(v){return fmt(v)+' ('+Math.round(v/total*1000)/10+'%)'}),textposition:'outside'}],L({showlegend:false,yaxis:{gridcolor:'#334155',title:'Unique Users'},margin:{t:30,b:120,l:60,r:20}}),RC);
var tb='';aLOC.forEach(function(r){var ln=r.l==='hi'?'Hindi':r.l==='en'?'English':r.l;tb+='<tr><td>'+r.p+'</td><td>'+ln+'</td><td>'+fmt(r.u)+'</td></tr>'});
document.querySelector('#tb_loc tbody').innerHTML=tb;
};

window.rt8=function(){
var fLC=filtArr(LC);
// Compute summary from filtered data
var fLCS={};fLC.forEach(function(r){fLCS[r.l]=(fLCS[r.l]||0)+r.u});
var toEn=fLCS['en']||0,toHi=fLCS['hi']||0,totalU=toEn+toHi;
var enPct=totalU>0?Math.round(toEn/totalU*1000)/10:0,hiPct=totalU>0?Math.round(toHi/totalU*1000)/10:0;
document.getElementById('k8').innerHTML=
'<div class="kpi purple"><div class="v">'+fmt(totalU)+'</div><div class="l">Total Unique Users Changed Lang</div></div>'+
'<div class="kpi"><div class="v">'+fmt(toEn)+' ('+enPct+'%)</div><div class="l">Switched to English</div></div>'+
'<div class="kpi orange"><div class="v">'+fmt(toHi)+' ('+hiPct+'%)</div><div class="l">Switched to Hindi</div></div>';
// Aggregate by page
var pg={};fLC.forEach(function(r){if(!pg[r.p])pg[r.p]={u:0,hi:0,en:0,unk:0,c:0};pg[r.p].u+=r.u;pg[r.p].c+=r.c;if(r.l==='hi')pg[r.p].hi+=r.u;else if(r.l==='en')pg[r.p].en+=r.u;else pg[r.p].unk+=r.u});
var pArr=Object.keys(pg).map(function(p){return{p:p,u:pg[p].u,hi:pg[p].hi,en:pg[p].en,unk:pg[p].unk,c:pg[p].c}}).sort(function(a,b){return b.u-a.u});
var top10=pArr.slice(0,10);
var pgTotal=pArr.reduce(function(a,r){return a+r.u},0)||1;
Plotly.newPlot('c_lcpage',[{y:top10.map(function(r){return r.p}).reverse(),x:top10.map(function(r){return r.u}).reverse(),type:'bar',orientation:'h',marker:{color:'#a855f7'},text:top10.map(function(r){return fmt(r.u)+' ('+Math.round(r.u/pgTotal*1000)/10+'%)'}).reverse(),textposition:'outside'}],L({showlegend:false,margin:{t:20,b:40,l:200,r:80},xaxis:{gridcolor:'#334155',title:'Unique Users'}}),RC);
Plotly.newPlot('c_lclang',[{x:top10.map(function(r){return r.p}),y:top10.map(function(r){return r.hi}),name:'To Hindi',type:'bar',marker:{color:'#f59e0b'},text:top10.map(function(r){return fmt(r.hi)+' ('+Math.round(r.hi/(r.u||1)*1000)/10+'%)'}),textposition:'outside'},{x:top10.map(function(r){return r.p}),y:top10.map(function(r){return r.en}),name:'To English',type:'bar',marker:{color:'#3b82f6'},text:top10.map(function(r){return fmt(r.en)+' ('+Math.round(r.en/(r.u||1)*1000)/10+'%)'}),textposition:'outside'}],L({barmode:'group',yaxis:{gridcolor:'#334155',title:'Unique Users'},margin:{t:30,b:120,l:60,r:20}}),RC);
// Table - aggregate filtered data for table display
var tbl={};fLC.forEach(function(r){var k=r.p+'|'+r.l;if(!tbl[k])tbl[k]={p:r.p,l:r.l,c:0,u:0};tbl[k].c+=r.c;tbl[k].u+=r.u});
var tblArr=Object.values(tbl).sort(function(a,b){return b.u-a.u});
var tb='';tblArr.forEach(function(r){var ln=r.l==='hi'?'Hindi':r.l==='en'?'English':r.l;tb+='<tr><td>'+r.p+'</td><td>'+ln+'</td><td>'+fmt(r.c)+'</td><td>'+fmt(r.u)+'</td></tr>'});
document.querySelector('#tb_lc tbody').innerHTML=tb;
};

// Init comparison tab
window.rt9=function(){
var cv1=document.getElementById('cv1'),cv2=document.getElementById('cv2');
if(cv1.options.length<=1){cv1.innerHTML='';cv2.innerHTML='';
AV.forEach(function(v){cv1.innerHTML+='<option value="'+v+'">'+v+'</option>';cv2.innerHTML+='<option value="'+v+'">'+v+'</option>'});
if(AV.length>1)cv2.selectedIndex=AV.length-1}
document.getElementById('cdf1').value=MIND;document.getElementById('cdt1').value=MAXD;
document.getElementById('cdf2').value=MIND;document.getElementById('cdt2').value=MAXD;
};

function cmpFunnel(sA,sB,chartA,chartB,tblId,titleA,titleB,lA,lB){
var vA=FK.map(function(k){return sA[k]||0}),vB=FK.map(function(k){return sB[k]||0});
var iA=sA.installs||1,iB=sB.installs||1;
Plotly.newPlot(chartA,[{type:'funnel',y:FL,x:vA,textinfo:'value+percent initial',marker:{color:FC},connector:{line:{color:'#334155'}}}],L({margin:{t:20,b:20,l:160,r:60},showlegend:false}),RC);
Plotly.newPlot(chartB,[{type:'funnel',y:FL,x:vB,textinfo:'value+percent initial',marker:{color:FC.map(function(c){return c.replace('f6','96').replace('f1','91')})},connector:{line:{color:'#334155'}}}],L({margin:{t:20,b:20,l:160,r:60},showlegend:false}),RC);
document.getElementById(titleA).textContent=lA+' Funnel';
document.getElementById(titleB).textContent=lB+' Funnel';
var tb='';FK.forEach(function(k,i){var a=sA[k]||0,b=sB[k]||0,pA=Math.round(a/iA*1000)/10,pB=Math.round(b/iB*1000)/10;
var df=Math.round((pB-pA)*10)/10,cl=df>0?'#22c55e':df<0?'#ef4444':'#94a3b8';
tb+='<tr><td style="font-weight:600">'+FL[i]+'</td><td>'+fmt(a)+'</td><td>'+pA+'%</td><td>'+fmt(b)+'</td><td>'+pB+'%</td><td style="color:'+cl+';font-weight:600">'+(df>0?'+':'')+df+'%</td></tr>'});
document.querySelector('#'+tblId+' tbody').innerHTML=tb;
}

function cmpDates(){
var f1=document.getElementById('cdf1').value,t1=document.getElementById('cdt1').value;
var f2=document.getElementById('cdf2').value,t2=document.getElementById('cdt2').value;
var dA=CD.filter(function(r){return r.d>=f1&&r.d<=t1}),dB=CD.filter(function(r){return r.d>=f2&&r.d<=t2});
var sA=agg(dA),sB=agg(dB);
cmpFunnel(sA,sB,'c_cmpA','c_cmpB','tb_cmp','cmpATitle','cmpBTitle',f1+' to '+t1,f2+' to '+t2);
}

function cmpVersions(){
var v1=document.getElementById('cv1').value,v2=document.getElementById('cv2').value;
var sA=null,sB=null;
VD.forEach(function(r){if(r.v===v1)sA=r;if(r.v===v2)sB=r});
if(!sA||!sB){alert('Version data not found');return}
cmpFunnel(sA,sB,'c_cmpVA','c_cmpVB','tb_vcmp','cmpVATitle','cmpVBTitle',v1,v2);
}

// Refresh: reload page to get latest data from GitHub Pages (auto-updates every 15 min)
function refreshAll(){
var ov=document.getElementById('refreshOverlay'),msg=document.getElementById('refreshMsg'),det=document.getElementById('refreshDetail');
document.getElementById('refreshBtn').disabled=true;ov.classList.add('show');
msg.textContent='Loading latest data...';
det.textContent='Dashboard auto-updates every 15 minutes via GitHub Actions';
setTimeout(function(){window.location.reload(true)},1500);
}

render();
</script></body></html>"""

out_path = os.path.join(OUT, 'index.html')
with open(out_path, 'w', encoding='utf-8') as f:
    f.write(html)
print(f"\nDashboard saved to: {out_path}")

main_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'booking_dashboard.html')
with open(main_path, 'w', encoding='utf-8') as f:
    f.write(html)
print(f"Also saved to: {main_path}")
print("Done!")
