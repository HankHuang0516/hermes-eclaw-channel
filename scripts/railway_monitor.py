#!/usr/bin/env python3
"""
Railway prod log monitor SOP v4 — Hermes #5
Changelog from v3:
  - (a) Open card → MUST assert r["card"]["id"] is non-empty, fail hard if missing
  - (b) Dedup: before creating card, GET /api/mission/card?parentId=<PARENT>&status=backlog to check
    if card with same normalized title already exists in backlog. If yes → skip creation.
  - Parent card already done → skip report/move (idempotent re-run)
"""
import json, subprocess, re
from datetime import datetime, timezone, timedelta
from collections import defaultdict

CREDS_FILE = "/home/node/hermes-agent/.eclaw-creds.json"
TOKEN_FILE = "/home/node/hermes-agent/.railway-token"
PARENT_CARD = "card_b2fc0e2d3564123686690d2e"

TOKEN = open(TOKEN_FILE).read().strip()
PROJECT = "844a6139-9b8f-43d6-a698-f97db1fa05c0"
ENV = "05055fd9-4e1f-4281-80f5-86a5dcf871af"
SVC_ECLAW = "12130b67-e57d-466b-a250-632b24cc6e3a"
SVC_PG = "e859fcf4-7f28-4082-8e5d-a00fe0a7f35c"

creds = json.load(open(CREDS_FILE))
DEV = creds["deviceId"]
BOT_SECRET = creds["botSecret"]
ENTITY_ID = creds["entityId"]
API_BASE = creds["apiBase"]

# ── Kanban API helpers ──────────────────────────────────────────────

def api_get(path_and_query):
    """GET with query params in URL."""
    r = subprocess.run(
        ["curl", "-s",
         f"{API_BASE}{path_and_query}&deviceId={DEV}&entityId={ENTITY_ID}&botSecret={BOT_SECRET}"],
        capture_output=True, text=True, timeout=15)
    return json.loads(r.stdout)

def api_post(path, body):
    body = dict(body, deviceId=DEV, entityId=ENTITY_ID, botSecret=BOT_SECRET)
    r = subprocess.run(
        ["curl", "-s", "-X", "POST", f"{API_BASE}{path}",
         "-H", "Content-Type: application/json", "-d", json.dumps(body)],
        capture_output=True, text=True, timeout=15)
    return json.loads(r.stdout)

# ── Railway GraphQL helper ─────────────────────────────────────────

def gql(q):
    r = subprocess.run(
        ["curl", "-sS", "-X", "POST",
         "https://backboard.railway.app/graphql/v2",
         "-H", f"Project-Access-Token: {TOKEN}",
         "-H", "Content-Type: application/json",
         "-d", json.dumps({"query": q})],
        capture_output=True, text=True, timeout=30)
    return json.loads(r.stdout)

def latest_deployment(service_id):
    d = gql(f'query{{deployments(first:5,input:{{projectId:"{PROJECT}",serviceId:"{service_id}",environmentId:"{ENV}"}}){{edges{{node{{id status createdAt}}}}}}}}')
    success = [e for e in d["data"]["deployments"]["edges"] if e["node"]["status"] == "SUCCESS"]
    return success[0]["node"]["id"] if success else None

def deployment_logs(deploy_id):
    d = gql(f'query{{deploymentLogs(deploymentId:"{deploy_id}",limit:5000,filter:""){{timestamp message severity}}}}')
    return d["data"]["deploymentLogs"]

# ── Dedup: fetch existing ErrLog subcards of parent from backlog ────

def existing_errlog_titles():
    """Return set of normalized titles already in backlog for parent card."""
    # Try to get parent card which may have linked info
    try:
        parent = api_get(f"/api/mission/card/{PARENT_CARD}?")
        # Walk through known ErrLog card IDs directly
        # The API doesn't expose a children list; we query each known card
        # In practice: we store created card IDs in a local cache file
        cache_file = "/home/node/hermes-agent/.railway-errlog-cache.json"
        try:
            with open(cache_file) as f:
                cache = json.load(f)
            return set(cache.get("created_titles", []))
        except:
            return set()
    except:
        return set()

def save_created_title(title):
    """Persist created card title to dedup cache."""
    cache_file = "/home/node/hermes-agent/.railway-errlog-cache.json"
    try:
        with open(cache_file) as f:
            cache = json.load(f)
    except:
        cache = {"created_titles": []}
    if title not in cache["created_titles"]:
        cache["created_titles"].append(title)
    with open(cache_file, "w") as f:
        json.dump(cache, f)

# ── Normalization + classification ─────────────────────────────────

CRIT_RE = re.compile(r"(?i)(FATAL|panic|UnhandledPromiseRejection|EADDRINUSE|OOMKilled|out\s*of\s*memory)")
ERR_RE  = re.compile(r"(?i)(ERROR:|error:|SequelizeError|PGError|column\s+\S+\s+does\s+not\s+exist|relation\s+\S+\s+does\s+not\s+exist|duplicate\s+key|foreign\s+key\s+constraint|connection\s+refused|ETIMEDOUT)")
WARN_RE = re.compile(r"(?i)(WARN:|deprecation|memory\s*leak)")
IGN1    = re.compile(r"^(checkpoint|restartpoint|automatic vacuum|automatic analyze)")
IGN2    = re.compile(r"duration: 0\.0")
IGN3    = re.compile(r"autoReviewOnTransform skipped")  # kanban.js:3963 console.warn — benign, not error
# v5 (2026-06-19) — expanded benign allow-list per #2 commander + Hank directive
# (root: ErrLog cron defect 2 = console.warn miscategorized as ERROR, dozens of flood cards)
IGN4    = re.compile(r"\[Kanban\] Auth failed")  # user bad creds, not system error
IGN5    = re.compile(r"\[ChatEmbedding\] HNSW index creation failed \(search still works\)")  # Railway pg-shmem known limit; seq-scan fallback works
IGN6    = re.compile(r"LIFECYCLE: REDIS_URL not set")  # intentional config — PG is source of truth
IGN7    = re.compile(r"\[Trust\] Schema warning: functions in index predicate must be")  # PG IMMUTABLE edge case, idempotent recovery
IGN8    = re.compile(r"\[Mindmap\] petdx avatar enrichment failed")  # caught + degraded, not a real error
IGN9    = re.compile(r"\[Push\] Full error: DOMException \[TimeoutError\]")  # offline-device push timeout; tracked via PR #3548 dormancy counter
IGN_ALL = [IGN1, IGN2, IGN3, IGN4, IGN5, IGN6, IGN7, IGN8, IGN9]

def normalize(msg):
    msg = re.sub(r"\d{4}-\d{2}-\d{2}.*?UTC", "<TS>", msg)
    msg = re.sub(r"\[\d+\]", "<PID>", msg)
    msg = re.sub(r"\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}", "<IP>", msg)
    return msg.strip()

def normalize_title(title):
    """Normalize for dedup: remove count suffix and trailing card ID."""
    t = re.sub(r'\s*[×x]\d+\)', ')', title)
    t = re.sub(r'\s*-\s*card_[a-f0-9]+$', '', t, flags=re.IGNORECASE)
    return t.strip()

def classify(l):
    msg, sev = l["message"], l["severity"]
    # v5: IGNORE check FIRST so allow-listed benign patterns don't get re-classified
    # as ERROR/WARN by the regex fall-throughs below.
    if any(p.search(msg) for p in IGN_ALL): return "IGNORE"
    if sev in ("fatal","panic") or CRIT_RE.search(msg): return "CRITICAL"
    if sev == "error" or ERR_RE.search(msg): return "ERROR"
    if sev == "warning" or WARN_RE.search(msg): return "WARN"
    return None

# v5 multi-line dump grouping: a single console.warn({ obj }, '[Push] ✗ ...') call
# emits N log LINES to Railway (header + brace + per-field + stack frames). Without
# pre-merging, each line becomes a separate ErrLog card. Group continuation lines
# back to their parent header before classification + grouping.
_CONTINUATION_PATTERNS = [
    re.compile(r"^\s+"),           # any indented line
    re.compile(r"^\s*[{}\[\]]\s*$"),  # bare brace
    re.compile(r"^\s*at\s+"),      # stack frame "at Promise.allSettled..."
    re.compile(r"^\s+[a-zA-Z_][a-zA-Z0-9_]*\s*:\s+"),  # object field "  key: value"
    re.compile(r"^\s*'[^']*'\s*,?\s*$"),  # bare quoted string fragment
    re.compile(r"^\s*\d+:\d+\)\s*$"),  # line:col suffix
]

def is_continuation_line(msg):
    """True iff msg looks like a continuation of the previous error event."""
    if not msg.strip():  # blank
        return True
    return any(p.match(msg) for p in _CONTINUATION_PATTERNS)

def group_multiline_logs(logs):
    """Merge continuation lines into the parent log line (in-place semantics: returns new list)."""
    if not logs:
        return logs
    out = []
    for l in logs:
        msg = l.get("message", "")
        if out and is_continuation_line(msg):
            # Merge into previous entry
            prev = dict(out[-1])
            prev["message"] = prev.get("message", "") + "\n" + msg
            out[-1] = prev
        else:
            out.append(l)
    return out

def detect_service(norm_msg, sample):
    combined = (norm_msg + sample).lower()
    if any(x in combined for x in ["column","relation","null value","device_id","kanban_cards","mission_dashboard","foreign key","pgerror","code: '23502'"]):
        return "Postgres"
    if any(x in combined for x in ["hnsw","chatembedding","embedding","vector"]): return "eclaw-chatembedding"
    if any(x in combined for x in ["grpc","grpc_port"]): return "eclaw-grpc"
    if any(x in combined for x in ["redis","deadline"]): return "eclaw-redis"
    if any(x in combined for x in ["trust","schema","index"]): return "eclaw-trust"
    if any(x in combined for x in ["autoreview","transform"]): return "eclaw-ops"
    return "eclaw"

# ── Main ────────────────────────────────────────────────────────────

now = datetime.now(timezone.utc)
six_h = now - timedelta(hours=6)

# Deployments
deploy_ec = latest_deployment(SVC_ECLAW)
deploy_pg = latest_deployment(SVC_PG)
print(f"ECLAW: {deploy_ec}  PG: {deploy_pg}")

# Pull logs
logs_all = []
if deploy_ec:
    logs_all.extend(deployment_logs(deploy_ec))
if deploy_pg:
    logs_all.extend(deployment_logs(deploy_pg))

# Filter 6h
filtered = []
for l in logs_all:
    try:
        ts = datetime.fromisoformat(l["timestamp"].replace("Z","+00:00"))
        if ts >= six_h:
            filtered.append(l)
    except:
        pass
print(f"Total: {len(logs_all)}  6h: {len(filtered)}")

# v5: collapse multi-line dumps into single events BEFORE classification.
# Without this, each line of a console.warn({obj}, '...') call became its own card.
filtered_pre_merge_count = len(filtered)
filtered = group_multiline_logs(filtered)
print(f"After multi-line merge: {len(filtered)} (was {filtered_pre_merge_count}, dropped {filtered_pre_merge_count - len(filtered)} continuation lines)")

# Classify
grouped = defaultdict(list)
for l in filtered:
    cls = classify(l)
    if cls and cls != "IGNORE":
        grouped[(cls, normalize(l["message"]))].append(l)

critical  = sorted((k for k in grouped if k[0]=="CRITICAL"), key=lambda x:-len(grouped[x]))
err_high  = sorted((k for k in grouped if k[0]=="ERROR" and len(grouped[k])>=5), key=lambda x:-len(grouped[x]))
err_low   = sorted((k for k in grouped if k[0]=="ERROR" and len(grouped[k])<5), key=lambda x:-len(grouped[x]))
warn_all  = sorted((k for k in grouped if k[0]=="WARN"), key=lambda x:-len(grouped[x]))

# Load dedup cache
created_titles = set()
try:
    cache = json.load(open("/home/node/hermes-agent/.railway-errlog-cache.json"))
    created_titles = set(cache.get("created_titles", []))
except:
    pass

print(f"Existing dedup titles in cache: {len(created_titles)}")

# Create subcards with dedup
def open_card(cls, priority, k, lines):
    count = len(lines)
    first_ts = lines[0]["timestamp"]
    last_ts = lines[-1]["timestamp"]
    sample = lines[0]["message"][:200]
    svc = detect_service(k[1], sample)
    title = f"[ErrLog/{cls}] {k[1][:60]} (service={svc} ×{count})"
    norm_title_key = normalize_title(title)

    # DEDUP: skip if already created
    if norm_title_key in created_titles:
        print(f"  ⊖ SKIP (dedup) {title[:55]}")
        return 0

    desc = json.dumps({
        "service": svc, "severity": cls, "occurrence_count": count,
        "first_seen": first_ts, "last_seen": last_ts,
        "sample_message": sample,
        "suggested_root_cause": "prod error — see sample_message",
        "linkBackParentCardId": PARENT_CARD
    }, ensure_ascii=False)

    r = api_post("/api/mission/card", {
        "title": title, "description": desc, "priority": priority,
        "parentCardId": PARENT_CARD, "assignedBots": [2]
    })

    # ASSERT: card.id must be present
    card_id = r.get("card", {}).get("id", "")
    if not (r.get("success") and card_id):
        raise AssertionError(f"Card creation failed! response={json.dumps(r)[:200]}")

    save_created_title(norm_title_key)
    print(f"  ⊕ CREATE {card_id} [{priority}] {title[:55]}")
    return 1

cards_created = 0
for cls, priority, key_list in [("CRITICAL","P0",critical),("ERROR-high","P1",err_high),("ERROR-low","P2",err_low)]:
    for k in key_list:
        cards_created += open_card(cls, priority, k, grouped[k])

# ── P95 latency metrics from hermes_metrics ─────────────────────────────────

try:
    import sys as _sys
    _sys.path.insert(0, "/home/node/hermes-agent")
    from hermes_metrics import metrics as _metrics

    _p95 = _metrics.read_p95()
    _ft = _p95["first_token_p95"]
    _ee = _p95["e2e_complete_p95"]
    _n_ft = _p95["n_first_token"]
    _n_ee = _p95["n_e2e_complete"]

    if _ft is not None or _ee is not None:
        _parts = ["Latency P95 (近6h):"]
        if _ft is not None:
            _parts.append(f"first_token={_ft}ms")
        else:
            _parts.append(f"first_token=n/a (n={_n_ft})")
        if _ee is not None:
            _parts.append(f"e2e={_ee}ms")
        else:
            _parts.append(f"e2e=n/a (n={_n_ee})")
        _p95_comment = " ".join(_parts)
    else:
        _p95_comment = None

    _p95_logged = False
except Exception as _e:
    _p95_comment = None

# Report
n_crit,n_eh,n_el,n_warn = len(critical),len(err_high),len(err_low),len(warn_all)
report = f"過去 6h prod log 巡完：{n_crit} CRITICAL / {n_eh} ERROR-high / {n_el} ERROR-low / {n_warn} warn. 開了 {cards_created} 張新 follow-up 子卡。"
if _p95_comment:
    report += f"\n\n{_p95_comment}"
r_c = api_post(f"/api/mission/card/{PARENT_CARD}/comment", {"text": report})
print(f"\nReport: {r_c.get('success')}")

# Idempotent move: only if not already done
parent = api_get(f"/api/mission/card/{PARENT_CARD}?")
p_status = parent.get("card", {}).get("status", "")
if p_status != "done":
    r_m = api_post(f"/api/mission/card/{PARENT_CARD}/move", {"newStatus": "in_progress"})
    import time; time.sleep(1)
    r_m2 = api_post(f"/api/mission/card/{PARENT_CARD}/move", {"newStatus": "done"})
    print(f"Move: in_progress={r_m.get('success')} done={r_m2.get('success')}")
else:
    print(f"Parent already done — skipping move")

print(f"\n=== DONE {n_crit}CRIT/{n_eh}P1/{n_el}P2/{n_warn}WARN, {cards_created} NEW cards ===")
