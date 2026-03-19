"""
scrape_to_json.py
─────────────────
Scrapa il portale EU Funding & Tenders con Playwright e produce calls.json
direttamente, senza passare per Excel.
Incorpora tutta la logica di classificazione di make_calls_json.py.

Uso:
    python scrape_to_json.py              # scrive calls.json nella cartella corrente
    python scrape_to_json.py --out /path  # percorso custom
"""

import re
import math
import time
import json
import argparse
from datetime import datetime, timezone
from pathlib import Path

from playwright.sync_api import sync_playwright

# ── Parametri ─────────────────────────────────────────────────────────────────

PAGE_SIZE = 50

LIST_URL = (
    "https://ec.europa.eu/info/funding-tenders/opportunities/portal/screen"
    "/opportunities/calls-for-proposals"
    "?order=DESC&pageNumber={page}&pageSize={ps}&sortBy=startDate"
    "&isExactMatch=true&status=31094501,31094502&programmePeriod=2021%20-%202027"
)

SEARCH_API  = "search-api/prod/rest/search"
COOKIE_TEXT = "This site uses cookies"

LINK_SELECTOR = (
    'a[href*="/topic-details/"], '
    'a[href*="/competitive-calls-cs/"], '
    'a[href*="/prospect-details/"]'
)

RE_TOTAL     = re.compile(r"(\d+)\s*item\s*\(s\)\s*found", re.IGNORECASE)
RE_OPEN      = re.compile(r"Opening date:\s*([^\|\n\r]+)",          re.IGNORECASE)
RE_DEAD      = re.compile(r"Deadline date:\s*([^\|\n\r]+)",         re.IGNORECASE)
RE_NEXT_DEAD = re.compile(r"Next deadline:\s*([^\|\n\r]+)",         re.IGNORECASE)
RE_PROG      = re.compile(r"Programme:\s*([^\|\n\r]+)",             re.IGNORECASE)
RE_ACTION    = re.compile(r"Type of action:\s*([^\|\n\r]+)",        re.IGNORECASE)
RE_CLUSTER   = re.compile(r"HORIZON-CL([1-6])",                     re.IGNORECASE)
RE_CALL_ID   = re.compile(r"callIdentifier[=:\s]+([^\s&\|\n\r]+)",  re.IGNORECASE)

MONTHS = {
    "january":1,"february":2,"march":3,"april":4,"may":5,"june":6,
    "july":7,"august":8,"september":9,"october":10,"november":11,"december":12,
}

# ── Tabelle di classificazione (da make_calls_json.py) ────────────────────────

PROGRAMME_MAP = {
    "43108390":"Horizon Europe","43108391":"Horizon Europe",
    "43152860":"Digital Europe Programme","111111":"EU External Action-Prospect",
    "44181033":"European Defence Fund","43353764":"Erasmus+",
    "43251589":"CERV","43251814":"Creative Europe (CREA)",
    "43252476":"Single Market Programme (SMP)","43298664":"AGRIP",
    "43251842":"EUAF","43298916":"Euratom",
    "43089234":"Innovation Fund (INNOVFUND)","43637601":"PPPA",
    "44416173":"I3","45532249":"EUBA",
    "43252368":"Internal Security Fund (ISF)","43252449":"RFCS",
    "43298203":"UCPM","43254037":"European Solidarity Corps (ESC)",
    "44773066":"Just Transition Mechanism (JTM)",
    "43251567":"Connecting Europe Facility (CEF)",
    "43252386":"JUST","43252433":"Pericles IV","43252517":"SOCPL",
    "43253967":"RENEWFM","43254019":"European Social Fund+ (ESF+)",
    "43392145":"EMFAF",
}

THEMATIC_MAP = {
    "1":"Health & Life Sciences","2":"Culture, Creativity & Inclusion",
    "3":"Security & Resilience","4":"Digital, Industry & Space",
    "5":"Climate, Energy & Mobility","6":"Food, Bioeconomy & Environment",
    "M-CIT":"Climate-neutral & Smart Cities",
    "M-OCEAN":"Healthy Oceans, Seas, Coastal & Inland Waters",
}

PROGRAMME_THEMATIC_MAP = [
    ("European Defence Fund",           "Defence"),
    ("EDF",                             "Defence"),
    ("EU External Action",              "External Action & International Cooperation"),
    ("EU External Action-Prospect",     "External Action & International Cooperation"),
    ("Single Market Programme",         "SME, Entrepreneurship & Market Uptake"),
    ("CERV",                            "Culture, Creativity & Inclusion"),
    ("Creative Europe",                 "Culture, Creativity & Inclusion"),
    ("Erasmus+",                        "Culture, Creativity & Inclusion"),
    ("European Social Fund+",           "Culture, Creativity & Inclusion"),
    ("Just Transition",                 "Climate, Energy & Mobility"),
    ("Innovation Fund",                 "Climate, Energy & Mobility"),
    ("EMFAF",                           "Food, Bioeconomy & Environment"),
    ("LIFE",                            "Food, Bioeconomy & Environment"),
    ("Euratom",                         "Climate, Energy & Mobility"),
    ("Connecting Europe",               "Climate, Energy & Mobility"),
    ("Internal Security Fund",          "Security & Resilience"),
    ("European Solidarity Corps",       "Culture, Creativity & Inclusion"),
    ("Digital Europe",                  "Digital, Industry & Space"),
    ("RENEWFM",                         "Climate, Energy & Mobility"),
    ("SOCPL",                           "Culture, Creativity & Inclusion"),
    ("JUST",                            "Culture, Creativity & Inclusion"),
    ("Pericles IV",                     "Culture, Creativity & Inclusion"),
    ("I3",                              "SME, Entrepreneurship & Market Uptake"),
    ("ERC",                             "Cross-cutting / Other"),
    ("43392145",                        "Food, Bioeconomy & Environment"),
    ("Horizon Europe",                  "Cross-cutting / Other"),
]

# (prefix, subcode_or_None, cluster_num, cluster_label, thematic)
URL_RULES = [
    ("MISS","CIT",   "M-CIT", "Climate-neutral & Smart Cities",               "Climate-neutral & Smart Cities"),
    ("MISS","OCEAN", "M-OCEAN","Healthy Oceans, Seas, Coastal & Inland Waters","Healthy Oceans, Seas, Coastal & Inland Waters"),
    ("MISS","CLIMA", "5",     "Climate, Energy and Mobility",                  "Climate, Energy & Mobility"),
    ("MISS","CANCER","1",     "Health",                                        "Health & Life Sciences"),
    ("MISS","SOIL",  "6",     "Food, Bioeconomy, Natural Resources, Agriculture and Environment","Food, Bioeconomy & Environment"),
    ("MISS","CROSS", "",      "",                                              "Cross-cutting / Other"),
    ("HLTH",    None,"1",     "Health",                                        "Health & Life Sciences"),
    ("EIC",     None,"",      "",                                              "SME, Entrepreneurship & Market Uptake"),
    ("EIE",     None,"",      "",                                              "SME, Entrepreneurship & Market Uptake"),
    ("EIT",     None,"",      "",                                              "SME, Entrepreneurship & Market Uptake"),
    ("CID",     None,"5",     "Climate, Energy and Mobility",                  "Climate, Energy & Mobility"),
    ("EURATOM", None,"5",     "Climate, Energy and Mobility",                  "Climate, Energy & Mobility"),
    ("EUROHPC", None,"4",     "Digital, Industry and Space",                   "Digital, Industry & Space"),
    ("JU-CLEAN-AVIATION",None,"","",                                           "Clean Aviation"),
    ("JU-",     None,"",      "",                                              "Climate, Energy & Mobility"),
    ("MSCA",    None,"",      "",                                              "Cross-cutting / Other"),
    ("NEB",     None,"",      "",                                              "Climate-neutral & Smart Cities"),
    ("RAISE",   None,"",      "",                                              "Cross-cutting / Other"),
    ("WIDERA",  None,"",      "",                                              "Cross-cutting / Other"),
    ("INFRA",   None,"",      "",                                              "Cross-cutting / Other"),
    ("AGRIP",   None,"6",     "Food, Bioeconomy, Natural Resources, Agriculture and Environment","Food, Bioeconomy & Environment"),
    ("EUAF",    None,"",      "",                                              "Cross-cutting / Other"),
    ("DIGITAL", None,"4",     "Digital, Industry and Space",                   "Digital, Industry & Space"),
    ("UCPM",    None,"",      "",                                              "Cross-cutting / Other"),
    ("RFCS",    None,"5",     "Climate, Energy and Mobility",                  "Climate, Energy & Mobility"),
    ("EUBA",    None,"",      "",                                              "External Action & International Cooperation"),
    ("PPPA","CHIPS","4",      "Digital, Industry and Space",                   "Digital, Industry & Space"),
    ("PPPA","MEDIA","",       "",                                              "Culture, Creativity & Inclusion"),
    ("PPPA",    None,"4",     "Digital, Industry and Space",                   "Digital, Industry & Space"),
    ("RENEWFM", None,"5",     "Climate, Energy and Mobility",                  "Climate, Energy & Mobility"),
    ("SOCPL",   None,"",      "",                                              "Culture, Creativity & Inclusion"),
    ("ERC",     None,"",      "",                                              "Cross-cutting / Other"),
    ("EMFAF",   None,"6",     "Food, Bioeconomy, Natural Resources, Agriculture and Environment","Food, Bioeconomy & Environment"),
    ("JUST",    None,"",      "",                                              "Culture, Creativity & Inclusion"),
    ("I3",      None,"",      "",                                              "SME, Entrepreneurship & Market Uptake"),
]

URL_BENEFICIARY_OVERRIDE = {
    "MSCA":  ["Research organisation"],
    "INFRA": ["Research organisation"],
    "EUAF":  ["Research organisation"],
    "EUBA":  ["Public body"],
}

# ── Classificazione ───────────────────────────────────────────────────────────

def _topic_id(url: str) -> str:
    s = (url or "").upper().split("?")[0]
    for m in ["/TOPIC-DETAILS/", "/COMPETITIVE-CALLS-CS/"]:
        i = s.find(m)
        if i >= 0:
            return s[i + len(m):]
    return s

def url_classify(url: str):
    tid = _topic_id(url)
    for prefix, subcode, c_num, c_label, thematic in URL_RULES:
        if prefix not in tid:
            continue
        if subcode is not None:
            if f"-{subcode}-" not in tid and not tid.endswith(f"-{subcode}"):
                continue
        benef = URL_BENEFICIARY_OVERRIDE.get(prefix, None)
        return c_num, c_label, thematic, benef
    return "", "", "", None

def prog_thematic(prog: str) -> str:
    pl = (prog or "").lower()
    for key, label in PROGRAMME_THEMATIC_MAP:
        if key.lower() in pl:
            return label
    return ""

def resolve_thematic(cluster_num: str, prog: str) -> str:
    if cluster_num and THEMATIC_MAP.get(cluster_num):
        return THEMATIC_MAP[cluster_num]
    return prog_thematic(prog)

def normalize_action(v: str) -> str:
    s = (v or "").lower()
    if "research and innovation action" in s: return "RIA"
    if "innovation action" in s:              return "IA"
    if "coordination and support" in s:       return "CSA"
    if "cofund" in s:                         return "COFUND"
    return v or ""

def beneficiary_hint(action: str, prog: str, url_benef):
    if url_benef is not None:
        return url_benef
    a = (action or "").upper()
    p = (prog or "").lower()
    hints = []
    if a == "IA":   hints.extend(["SME","Large enterprise","Research organisation"])
    if a == "RIA":  hints.extend(["Research organisation","SME","Large enterprise"])
    if a == "CSA":  hints.extend(["Research organisation","Public body","NGO","SME"])
    if "external action" in p: hints.extend(["NGO","Public body","Research organisation"])
    return list(dict.fromkeys(hints))

# ── Parsing date ──────────────────────────────────────────────────────────────

def parse_date_iso(s: str) -> str:
    s = re.sub(r"\s+", " ", str(s or "")).strip()
    if not s:
        return ""
    m = re.search(r"\b(\d{4})-(\d{2})-(\d{2})\b", s)
    if m:
        return f"{m.group(1)}-{m.group(2)}-{m.group(3)}"
    m = re.search(r"\b(\d{1,2})[\/\-.](\d{1,2})[\/\-.](\d{4})\b", s)
    if m:
        try:
            return datetime(int(m.group(3)), int(m.group(2)), int(m.group(1))).strftime("%Y-%m-%d")
        except ValueError:
            pass
    m = re.search(r"\b(\d{1,2})\s+([A-Za-z]+)\s+(\d{4})\b", s)
    if m:
        mo = MONTHS.get(m.group(2).lower())
        if mo:
            try:
                return datetime(int(m.group(3)), mo, int(m.group(1))).strftime("%Y-%m-%d")
            except ValueError:
                pass
    return ""

# ── Utilità Playwright ────────────────────────────────────────────────────────

def clean(s):
    if not s:
        return None
    s = re.sub(r"\s+", " ", str(s)).strip()
    return s or None

def pick(rx, text):
    m = rx.search(text or "")
    return clean(m.group(1)) if m else None

def accept_cookies(page):
    for label in ["Accept all","Accept All","Accept","I accept","Agree","OK"]:
        for scope in [page] + list(page.frames):
            try:
                btn = scope.get_by_role("button", name=re.compile(label, re.IGNORECASE))
                if btn.count():
                    btn.first.click(timeout=2000)
                    page.wait_for_timeout(800)
                    return
            except Exception:
                pass

def wait_cookie_gone(page, max_ms=12000):
    t0 = time.time()
    while (time.time() - t0) * 1000 < max_ms:
        try:
            body = page.locator("body").inner_text()
        except Exception:
            body = ""
        if COOKIE_TEXT.lower() not in (body or "").lower():
            return
        page.wait_for_timeout(600)

def count_links(page):
    return page.locator(LINK_SELECTOR).count()

def read_total(page):
    txt = page.locator("body").inner_text()
    m = RE_TOTAL.search(txt or "")
    return int(m.group(1)) if m else None

def scroll_until(page, expected, max_ms=50000):
    start = time.time()
    last = -1
    stable_since = time.time()
    while count_links(page) == 0 and (time.time()-start)*1000 < 10000:
        accept_cookies(page)
        wait_cookie_gone(page, 3000)
        page.wait_for_timeout(700)
    container = page.evaluate_handle(f"""() => {{
        const sel = `{LINK_SELECTOR}`;
        const links = document.querySelectorAll(sel);
        if (!links.length) return null;
        let el = links[0];
        for (let i=0; i<20; i++) {{
            if (!el) break;
            const st = window.getComputedStyle(el);
            const oy = st.overflowY;
            if ((oy==='auto'||oy==='scroll') && el.scrollHeight>el.clientHeight+5) return el;
            el = el.parentElement;
        }}
        return null;
    }}""")
    while (time.time()-start)*1000 < max_ms:
        accept_cookies(page)
        wait_cookie_gone(page, 3000)
        c = count_links(page)
        if c >= expected:
            return c
        if c != last:
            last = c
            stable_since = time.time()
        try:
            if container:
                page.evaluate("(el)=>{ el.scrollTop = el.scrollTop + el.clientHeight*0.9; }", container)
            else:
                page.mouse.wheel(0, 1800)
        except Exception:
            pass
        page.wait_for_timeout(600)
        if time.time()-stable_since > 5:
            try:
                if container:
                    page.evaluate("(el)=>{ el.scrollTop = el.scrollHeight; }", container)
                else:
                    page.mouse.wheel(0, 5000)
            except Exception:
                pass
            page.wait_for_timeout(600)
    return count_links(page)

def extract_links(page):
    hrefs = page.evaluate(f"""
        () => Array.from(document.querySelectorAll('{LINK_SELECTOR}'))
                  .map(a => a.getAttribute('href'))
    """)
    out, seen = [], set()
    for h in hrefs or []:
        if not h:
            continue
        full = "https://ec.europa.eu" + h if h.startswith("/") else h
        if full not in seen:
            seen.add(full)
            out.append(full)
    return out

# ── Parsing card dalla lista ──────────────────────────────────────────────────

def parse_card(page, full_url: str) -> dict:
    path = full_url.replace("https://ec.europa.eu","").split("?")[0]
    a = page.locator(f'a[href*="{path}"]').first
    title = clean(a.inner_text()) if a.count() else path.split("/")[-1]

    card = a.locator(
        "xpath=ancestor::*[contains(.,'Programme:') or contains(.,'Opening date:') or "
        "contains(.,'Deadline date:') or contains(.,'Type of action:')][1]"
    ).first
    text = (card.inner_text() if card.count()
            else (a.locator("xpath=ancestor::*[1]").inner_text() if a.count() else ""))

    dead = pick(RE_DEAD, text) or pick(RE_NEXT_DEAD, text)
    call_id = pick(RE_CALL_ID, full_url) or pick(RE_CALL_ID, text)
    cluster_raw = pick(RE_CLUSTER, text) or pick(RE_CLUSTER, full_url) or pick(RE_CLUSTER, call_id or "")

    return {
        "name":           title,
        "call_id":        call_id,
        "programme_raw":  pick(RE_PROG, text),
        "action_raw":     pick(RE_ACTION, text),
        "cluster_raw":    cluster_raw,
        "opening_raw":    pick(RE_OPEN, text),
        "deadline_raw":   dead,
        "url":            full_url,
        "_needs_enrich":  False,
    }

# ── Arricchimento via XHR ────────────────────────────────────────────────────

def _first(meta, *keys):
    for k in keys:
        v = meta.get(k)
        if isinstance(v, list) and v:
            return re.sub(r"\s+", " ", str(v[0])).strip()
        if v and isinstance(v, str):
            return v.strip()
    return ""

def _enrich_one(page, row: dict) -> bool:
    """Apre una pagina di dettaglio e cattura i campi mancanti via XHR.
    Restituisce True se almeno un campo è stato recuperato."""
    url      = row["url"]
    captured = {}

    def handle(response, _c=captured):
        if SEARCH_API in response.url and response.status == 200:
            try:
                body = response.json()
                for item in body.get("results", [body]):
                    meta    = item.get("metadata", {}) or {}
                    prog_id = _first(meta, "frameworkProgramme", "programme")
                    action  = _first(meta, "typesOfAction","typeOfAction","fundingScheme")
                    cid     = _first(meta, "callIdentifier","identifier")
                    if prog_id and not _c.get("prog"):
                        _c["prog"] = PROGRAMME_MAP.get(prog_id, prog_id)
                    if action and not _c.get("action"):
                        _c["action"] = action
                    if cid and not _c.get("call_id"):
                        _c["call_id"] = cid
            except Exception:
                pass

    page.on("response", handle)
    try:
        page.goto(url, wait_until="domcontentloaded", timeout=30_000)
        page.wait_for_timeout(2500)
    except Exception as e:
        print(f"    [ERR goto] {e}", flush=True)
    finally:
        page.remove_listener("response", handle)

    if captured.get("prog") and not row.get("programme_raw"):
        row["programme_raw"] = captured["prog"]
    if captured.get("action") and not row.get("action_raw"):
        row["action_raw"] = captured["action"]
    if captured.get("call_id") and not row.get("call_id"):
        row["call_id"] = captured["call_id"]

    return bool(captured)


def enrich(ctx, rows: list):
    to_fix = [r for r in rows
              if (not r.get("programme_raw") or not r.get("action_raw") or not r.get("call_id"))
              and r.get("url")]
    if not to_fix:
        print("  Tutti i campi già presenti ✓", flush=True)
        return

    print(f"  {len(to_fix)} call da arricchire…", flush=True)
    page = ctx.new_page()
    skipped = 0

    for idx, row in enumerate(to_fix, 1):
        print(f"  [{idx:>4}/{len(to_fix)}] {(row['name'] or '')[:60]}", flush=True)

        ok = False
        for attempt in range(1, 3):   # max 2 tentativi per call
            try:
                ok = _enrich_one(page, row)
                break
            except Exception as e:
                print(f"    [tentativo {attempt} fallito] {e}", flush=True)
                # Ricrea la pagina se crashata
                try:
                    page.close()
                except Exception:
                    pass
                page = ctx.new_page()
                time.sleep(2)

        if not ok:
            skipped += 1
            print(f"    [SKIP] nessun dato recuperato", flush=True)

        # Salvataggio intermedio ogni 100 call (sicurezza in caso di crash)
        if idx % 100 == 0:
            print(f"  [checkpoint] salvate {idx} call finora…", flush=True)

        time.sleep(0.3)

    try:
        page.close()
    except Exception:
        pass
    print(f"  Arricchimento completato. Saltate: {skipped}/{len(to_fix)}", flush=True)

# ── Trasforma riga grezza → oggetto call classificato ─────────────────────────

def to_call(row: dict) -> dict:
    url        = row.get("url", "")
    prog_raw   = row.get("programme_raw") or ""
    call_id    = row.get("call_id") or ""
    action_raw = row.get("action_raw") or ""

    # Cluster: da call_id > cluster_raw > url
    cluster_num = ""
    for src in [call_id, row.get("cluster_raw",""), url]:
        m = RE_CLUSTER.search(src or "")
        if m:
            cluster_num = m.group(1)
            break

    # URL overrides
    u_cnum, u_clabel, u_thematic, u_benef = url_classify(url)
    if u_cnum:
        cluster_num = u_cnum

    cluster_label = u_clabel or THEMATIC_MAP.get(cluster_num, "")
    thematic      = u_thematic or resolve_thematic(cluster_num, prog_raw)
    action        = normalize_action(action_raw)
    is_mission    = bool("/HORIZON-MISS" in url.upper())

    opening_raw  = row.get("opening_raw") or ""
    deadline_raw = row.get("deadline_raw") or ""

    return {
        "name":             row.get("name") or "",
        "call_id":          call_id,
        "programme":        prog_raw,
        "cluster_num":      cluster_num,
        "cluster_label":    cluster_label,
        "thematic_cluster": thematic,
        "action":           action,
        "opening":          opening_raw,
        "opening_iso":      parse_date_iso(opening_raw),
        "deadline":         deadline_raw,
        "deadline_iso":     parse_date_iso(deadline_raw),
        "url":              url,
        "is_mission":       is_mission,
        "beneficiary_hint": beneficiary_hint(action, prog_raw, u_benef),
    }

# ── Main ──────────────────────────────────────────────────────────────────────

def main(out_path: Path):
    rows      = []
    seen_urls = set()

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        ctx = browser.new_context(
            locale="en-US",
            viewport={"width": 1920, "height": 1080},
            user_agent=(
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/122.0.0.0 Safari/537.36"
            ),
        )
        page = ctx.new_page()

        # ── Passo 1: lista ────────────────────────────────────────────────────
        page.goto(LIST_URL.format(page=1, ps=PAGE_SIZE),
                  wait_until="domcontentloaded", timeout=90000)
        page.wait_for_timeout(1500)
        accept_cookies(page)
        wait_cookie_gone(page)

        total = read_total(page)
        if total is None:
            print("❌ Non riesco a leggere 'item(s) found'.")
            browser.close()
            return
        max_pages = math.ceil(total / PAGE_SIZE)
        print(f"✅ Totale: {total} call | pagine: {max_pages}")

        for pnum in range(1, max_pages + 1):
            remaining = total - (pnum - 1) * PAGE_SIZE
            expected  = min(PAGE_SIZE, remaining)
            url = LIST_URL.format(page=pnum, ps=PAGE_SIZE)
            print(f"\n[p{pnum}/{max_pages}] attese ~{expected}", end="", flush=True)
            page.goto(url, wait_until="domcontentloaded", timeout=90000)
            page.wait_for_timeout(1200)
            accept_cookies(page)
            wait_cookie_gone(page)

            scroll_until(page, expected=expected)
            links     = extract_links(page)
            new_links = [u for u in links if u not in seen_urls]
            print(f" → trovati {len(new_links)} nuovi", flush=True)

            for u in new_links:
                seen_urls.add(u)
                rows.append(parse_card(page, u))
            time.sleep(0.1)

        # ── Passo 2: arricchimento ────────────────────────────────────────────
        needs = [r for r in rows if not r.get("programme_raw") or not r.get("action_raw") or not r.get("call_id")]
        print(f"\n═══ Passo 2: arricchimento {len(needs)} call su {len(rows)} totali ═══", flush=True)
        enrich(ctx, rows)
        browser.close()

    # ── Classificazione e output ──────────────────────────────────────────────
    calls = []
    seen  = set()
    for row in rows:
        call = to_call(row)
        if call["url"] and call["url"] not in seen:
            seen.add(call["url"])
            calls.append(call)

    # Statistiche
    tc = {}
    for c in calls:
        k = c["thematic_cluster"] or "(non classificato)"
        tc[k] = tc.get(k, 0) + 1
    print(f"\nClassificazione ({len(calls)} call totali):")
    for k, v in sorted(tc.items(), key=lambda x: -x[1]):
        print(f"  {v:5d}  {k}")
    print(f"\nNon classificati: {tc.get('(non classificato)', 0)}")

    payload = {
        "generated": datetime.now(timezone.utc).isoformat(),
        "calls": calls,
    }
    out_path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    print(f"\n Scritto {out_path} con {len(calls)} call")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--out", default="calls.json", help="Percorso output JSON")
    args = parser.parse_args()
    main(Path(args.out))
