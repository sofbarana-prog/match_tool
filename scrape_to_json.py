"""
scrape_to_json.py

Crea calls.json dal portale Funding & Tenders.
Per ogni call salva anche:
- fulltext
- search_blob
- subtopics

Uso:
    python scrape_to_json.py
    python scrape_to_json.py --out calls.json
"""

import re
import math
import json
import time
import argparse
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from playwright.sync_api import sync_playwright, Page, BrowserContext


PAGE_SIZE = 50

LIST_URL = (
    "https://ec.europa.eu/info/funding-tenders/opportunities/portal/screen"
    "/opportunities/calls-for-proposals"
    "?order=DESC&pageNumber={page}&pageSize={ps}&sortBy=startDate"
    "&isExactMatch=true&status=31094501,31094502&programmePeriod=2021%20-%202027"
)

COOKIE_TEXT = "This site uses cookies"
SEARCH_API_PART = "search-api/prod/rest/search"

LINK_SELECTOR = (
    'a[href*="/topic-details/"], '
    'a[href*="/competitive-calls-cs/"], '
    'a[href*="/prospect-details/"]'
)

DETAIL_TEXT_SELECTORS = [
    "main",
    '[role="main"]',
    "app-topic-details",
    "app-prospect-details",
    "app-competitive-call-details",
    "section",
]

NOISE_PATTERNS = [
    r"This site uses cookies.*",
    r"Accept all",
    r"Accept All",
    r"EU Funding & Tenders Portal",
    r"Reference Documents",
    r"Topic updates",
    r"Download PDF version",
    r"Print",
    r"Share",
    r"Go back",
]

RE_TOTAL = re.compile(r"(\d+)\s*item\s*\(s\)\s*found", re.IGNORECASE)
RE_OPEN = re.compile(r"Opening date:\s*([^\|\n\r]+)", re.IGNORECASE)
RE_DEAD = re.compile(r"Deadline date:\s*([^\|\n\r]+)", re.IGNORECASE)
RE_NEXT_DEAD = re.compile(r"Next deadline:\s*([^\|\n\r]+)", re.IGNORECASE)
RE_PROG = re.compile(r"Programme:\s*([^\|\n\r]+)", re.IGNORECASE)
RE_ACTION = re.compile(r"Type of action:\s*([^\|\n\r]+)", re.IGNORECASE)
RE_CLUSTER = re.compile(r"HORIZON-CL([1-6])", re.IGNORECASE)
RE_CALL_ID = re.compile(r"callIdentifier[=:\s]+([^\s&\|\n\r]+)", re.IGNORECASE)

MONTHS = {
    "january": 1, "february": 2, "march": 3, "april": 4,
    "may": 5, "june": 6, "july": 7, "august": 8,
    "september": 9, "october": 10, "november": 11, "december": 12,
}

PROGRAMME_MAP = {
    "43108390": "Horizon Europe",
    "43108391": "Horizon Europe",
    "43152860": "Digital Europe Programme",
    "111111": "EU External Action-Prospect",
    "44181033": "European Defence Fund",
    "43353764": "Erasmus+",
    "43251589": "CERV",
    "43251814": "Creative Europe (CREA)",
    "43252476": "Single Market Programme (SMP)",
    "43298664": "AGRIP",
    "43251842": "EUAF",
    "43298916": "Euratom",
    "43089234": "Innovation Fund (INNOVFUND)",
    "43637601": "PPPA",
    "44416173": "I3",
    "45532249": "EUBA",
    "43252368": "Internal Security Fund (ISF)",
    "43252449": "RFCS",
    "43298203": "UCPM",
    "43254037": "European Solidarity Corps (ESC)",
    "44773066": "Just Transition Mechanism (JTM)",
    "43251567": "Connecting Europe Facility (CEF)",
    "43252386": "JUST",
    "43252433": "Pericles IV",
    "43252517": "SOCPL",
    "43253967": "RENEWFM",
    "43254019": "European Social Fund+ (ESF+)",
    "43392145": "EMFAF",
}

THEMATIC_MAP = {
    "1": "Health & Life Sciences",
    "2": "Culture, Creativity & Inclusion",
    "3": "Security & Resilience",
    "4": "Digital, Industry & Space",
    "5": "Climate, Energy & Mobility",
    "6": "Food, Bioeconomy & Environment",
    "M-CIT": "Climate-neutral & Smart Cities",
    "M-OCEAN": "Healthy Oceans, Seas, Coastal & Inland Waters",
}

PROGRAMME_THEMATIC_MAP = [
    ("European Defence Fund", "Defence"),
    ("EDF", "Defence"),
    ("EU External Action", "External Action & International Cooperation"),
    ("EU External Action-Prospect", "External Action & International Cooperation"),
    ("Single Market Programme", "SME, Entrepreneurship & Market Uptake"),
    ("CERV", "Culture, Creativity & Inclusion"),
    ("Creative Europe", "Culture, Creativity & Inclusion"),
    ("Erasmus+", "Culture, Creativity & Inclusion"),
    ("European Social Fund+", "Culture, Creativity & Inclusion"),
    ("Just Transition", "Climate, Energy & Mobility"),
    ("Innovation Fund", "Climate, Energy & Mobility"),
    ("EMFAF", "Food, Bioeconomy & Environment"),
    ("LIFE", "Food, Bioeconomy & Environment"),
    ("Euratom", "Climate, Energy & Mobility"),
    ("Connecting Europe", "Climate, Energy & Mobility"),
    ("Internal Security Fund", "Security & Resilience"),
    ("European Solidarity Corps", "Culture, Creativity & Inclusion"),
    ("Digital Europe", "Digital, Industry & Space"),
    ("RENEWFM", "Climate, Energy & Mobility"),
    ("SOCPL", "Culture, Creativity & Inclusion"),
    ("JUST", "Culture, Creativity & Inclusion"),
    ("Pericles IV", "Culture, Creativity & Inclusion"),
    ("I3", "SME, Entrepreneurship & Market Uptake"),
    ("EUBA", "External Action & International Cooperation"),
    ("Horizon Europe", "Cross-cutting / Other"),
]

URL_RULES = [
    ("MISS", "CIT", "M-CIT", "Climate-neutral & Smart Cities", "Climate-neutral & Smart Cities"),
    ("MISS", "OCEAN", "M-OCEAN", "Healthy Oceans, Seas, Coastal & Inland Waters", "Healthy Oceans, Seas, Coastal & Inland Waters"),
    ("MISS", "CLIMA", "5", "Climate, Energy and Mobility", "Climate, Energy & Mobility"),
    ("MISS", "CANCER", "1", "Health", "Health & Life Sciences"),
    ("MISS", "SOIL", "6", "Food, Bioeconomy, Natural Resources, Agriculture and Environment", "Food, Bioeconomy & Environment"),
    ("MISS", "CROSS", "", "", "Cross-cutting / Other"),
    ("HLTH", None, "1", "Health", "Health & Life Sciences"),
    ("EIC", None, "", "", "SME, Entrepreneurship & Market Uptake"),
    ("EIE", None, "", "", "SME, Entrepreneurship & Market Uptake"),
    ("EITUM-BP", None, "M-CIT", "Climate-neutral & Smart Cities", "Climate-neutral & Smart Cities"),
    ("EIT", None, "", "", "SME, Entrepreneurship & Market Uptake"),
    ("CID", None, "5", "Climate, Energy and Mobility", "Climate, Energy & Mobility"),
    ("EURATOM", None, "5", "Climate, Energy and Mobility", "Climate, Energy & Mobility"),
    ("EUROHPC", None, "4", "Digital, Industry and Space", "Digital, Industry & Space"),
    ("JU-CLEAN-AVIATION", None, "", "", "Clean Aviation"),
    ("JU-", None, "", "", "Climate, Energy & Mobility"),
    ("MSCA", None, "", "", "Cross-cutting / Other"),
    ("NEB", None, "", "", "Climate-neutral & Smart Cities"),
    ("RAISE", None, "4", "Digital, Industry and Space", "Digital, Industry & Space"),
    ("WIDERA", None, "", "", "Cross-cutting / Other"),
    ("CL3", "INFRA", "3", "Civil Security for Society", "Security & Resilience"),
    ("INFRA", "TECH", "4", "Digital, Industry and Space", "Digital, Industry & Space"),
    ("INFRA", "SERV", "4", "Digital, Industry and Space", "Digital, Industry & Space"),
    ("INFRA", "DEV", "", "", "Research, Academia"),
    ("INFRA", "EOSC", "", "", "Research, Academia"),
    ("INFRA", None, "", "", "Research, Academia"),
    ("AGRIP", None, "6", "Food, Bioeconomy, Natural Resources, Agriculture and Environment", "Food, Bioeconomy & Environment"),
    ("EUAF", None, "4", "Digital, Industry & Space", "Digital, Industry & Space"),
    ("DIGITAL", None, "4", "Digital, Industry & Space", "Digital, Industry & Space"),
    ("UCPM", None, "", "", "Cross-cutting / Other"),
    ("RFCS", None, "5", "Climate, Energy and Mobility", "Climate, Energy & Mobility"),
    ("PPPA", "CHIPS", "4", "Digital, Industry & Space", "Digital, Industry & Space"),
    ("PPPA", "MEDIA", "", "", "Culture, Creativity & Inclusion"),
    ("PPPA", None, "4", "Digital, Industry & Space", "Digital, Industry & Space"),
    ("RENEWFM", None, "5", "Climate, Energy and Mobility", "Climate, Energy & Mobility"),
    ("SOCPL", None, "", "", "Culture, Creativity & Inclusion"),
    ("ERC", None, "", "", "Research, Academia"),
    ("EMFAF", None, "6", "Food, Bioeconomy, Natural Resources, Agriculture and Environment", "Food, Bioeconomy & Environment"),
    ("JUST", None, "", "", "Culture, Creativity & Inclusion"),
    ("I3", None, "", "", "SME, Entrepreneurship & Market Uptake"),
]

NAME_THEMATIC_RULES = [
    ("OHAMR", "Health & Life Sciences"),
    ("ERA4HEALTH", "Health & Life Sciences"),
    ("ERA4 HEALTH", "Health & Life Sciences"),
    ("EP BRAINHEALTH", "Health & Life Sciences"),
    ("BRAINHEALTH", "Health & Life Sciences"),
    ("ERDERA", "Health & Life Sciences"),
    ("EITUM-BP23-25", "Climate-neutral & Smart Cities"),
    ("URBAN MOBILITY", "Climate-neutral & Smart Cities"),
    ("EIC AWARDEE", "SME, Entrepreneurship & Market Uptake"),
    ("INNOMATCH", "SME, Entrepreneurship & Market Uptake"),
]

MANUAL_OVERRIDES = [
    {"match": "RAISE", "cluster": "Digital, Industry & Space", "subtopic": "digital"},
    {"match": "INFRADEV", "cluster": "Research, Academia", "subtopic": "research_infrastructures"},
    {"match": "INFRATECH", "cluster": "Digital, Industry & Space", "subtopic": "digital"},
    {"match": "INFRAEOSC", "cluster": "Research, Academia", "subtopic": "open_science"},
    {"match": "INFRASERV", "cluster": "Digital, Industry & Space", "subtopic": "digital"},
    {"match": "EUAF", "cluster": "Digital, Industry & Space", "subtopic": "digital"},
    {"match": "/12982?", "cluster": "Health & Life Sciences", "subtopic": "health"},
    {"match": "/12821?", "cluster": "SME, Entrepreneurship & Market Uptake", "subtopic": "market_uptake"},
    {"match": "OHAMR", "cluster": "Health & Life Sciences", "subtopic": "health"},
    {"match": "ERA4HEALTH", "cluster": "Health & Life Sciences", "subtopic": "health"},
    {"match": "EP BRAINHEALTH", "cluster": "Health & Life Sciences", "subtopic": "health"},
    {"match": "ERDERA", "cluster": "Health & Life Sciences", "subtopic": "health"},
    {"match": "EITUM-BP23-25", "cluster": "Climate-neutral & Smart Cities", "subtopic": "cities"},
]

TOPIC_CATALOG: Dict[str, Dict[str, List[str]]] = {
    "Health & Life Sciences": {
        "health": ["health", "healthcare", "medical", "clinical", "patient", "hospital"],
        "cancer": ["cancer", "oncology", "tumor", "tumour"],
        "biotech": ["biotech", "biotechnology", "biological", "biomanufacturing"],
        "pharma": ["pharma", "pharmaceutical", "drug", "therapy", "therapeutic"],
        "medtech": ["medical device", "medical devices", "implant", "wearable medical"],
        "diagnostics": ["diagnostic", "diagnostics", "screening", "biomarker"],
        "digital_health": ["digital health", "telemedicine", "remote monitoring", "health data"],
        "genomics": ["genomic", "genomics", "gene", "genetic", "dna", "rna"],
        "neuroscience": ["brain", "neuro", "neuroscience", "parkinson", "alzheimer"],
        "public_health": ["public health", "prevention", "epidemiology", "population health"],
    },
    "Culture, Creativity & Inclusion": {
        "heritage": ["heritage", "cultural heritage", "museum", "archive", "archaeology"],
        "creative_industries": ["creative industry", "creative industries", "design", "media", "audiovisual"],
        "inclusion": ["inclusion", "social inclusion", "inequality", "participation", "accessibility"],
        "democracy": ["democracy", "civic", "governance", "citizen engagement"],
        "education_society": ["education", "skills", "lifelong learning", "social innovation"],
        "migration": ["migration", "migrant", "refugee", "integration"],
    },
    "Security & Resilience": {
        "cybersecurity": ["cybersecurity", "cyber security", "cyberattack", "ransomware", "malware"],
        "disaster_resilience": ["disaster", "resilience", "emergency", "crisis response", "preparedness"],
        "border_security": ["border", "customs", "surveillance", "cross-border control"],
        "civil_protection": ["civil protection", "first responder", "public safety", "rescue"],
        "critical_infrastructure": ["critical infrastructure", "infrastructure protection", "risk assessment"],
        "urban_security": ["urban security", "peri urban", "urban resilience"],
    },
    "Digital, Industry & Space": {
        "digital": ["digital", "digitisation", "digitization", "software", "platform", "data tool"],
        "ai": ["artificial intelligence", "machine learning", "generative ai", "foundation model"],
        "robotics": ["robot", "robotics", "cobot", "autonomous system"],
        "semiconductors": ["semiconductor", "chip", "microelectronics", "photonics", "integrated circuit"],
        "quantum": ["quantum", "qubit", "quantum computing", "quantum communication"],
        "data_cloud": ["cloud", "edge", "data space", "data sharing", "interoperability"],
        "manufacturing": ["manufacturing", "factory", "industry 4.0", "advanced manufacturing"],
        "materials": ["advanced material", "materials", "composite", "alloy", "nanomaterial"],
        "space": ["space", "satellite", "launcher", "earth observation", "copernicus", "galileo"],
    },
    "Climate, Energy & Mobility": {
        "climate": ["climate", "climatic", "adaptation", "mitigation", "net zero", "carbon", "co2", "ghg", "emission"],
        "energy": ["energy", "energies", "electricity", "power system", "grid", "electrification"],
        "hydrogen": ["hydrogen", "h2", "fuel cell", "electrolyser", "electrolysis"],
        "batteries": ["battery", "batteries", "cell", "bms", "accumulator"],
        "mobility": ["mobility", "vehicle", "vehicles", "automotive", "ev", "electric vehicle"],
        "transport": ["transport", "rail", "aviation", "shipping", "maritime", "logistics"],
        "renewables": ["renewable", "solar", "pv", "photovoltaic", "wind", "geothermal", "hydropower", "biofuel"],
        "storage": ["storage", "energy storage", "thermal storage", "long-duration storage"],
        "smart_grids": ["smart grid", "smart grids", "demand response", "grid flexibility"],
        "buildings": ["building", "buildings", "retrofit", "renovation", "built environment"],
        "cities": ["city", "cities", "urban", "smart city", "smart cities"],
        "circularity": ["circular", "recycling", "reuse", "remanufacturing"],
        "water": ["water", "wastewater", "aquatic", "coastal", "marine"],
    },
    "Food, Bioeconomy & Environment": {
        "agriculture": ["agriculture", "farming", "crop", "precision farming", "agronomy"],
        "food_systems": ["food system", "food systems", "food processing", "nutrition", "food supply"],
        "bioeconomy": ["bioeconomy", "biobased", "bio-based", "biomass", "biorefinery"],
        "biodiversity": ["biodiversity", "ecosystem", "habitat", "species restoration"],
        "forestry": ["forest", "forestry", "wood", "silviculture"],
        "soil": ["soil", "regenerative soil", "land restoration", "soil health"],
        "environment": ["environment", "pollution", "air quality", "contaminant"],
        "water_resources": ["water resource", "river basin", "groundwater", "freshwater"],
    },
    "Defence": {
        "defence_systems": ["defence system", "military system", "battlefield", "mission system"],
        "autonomy": ["autonomous", "unmanned", "uas", "uav", "drone"],
        "sensing": ["radar", "sonar", "sensor fusion", "detection"],
        "secure_comm": ["secure communication", "encrypted communication", "tactical network"],
    },
    "SME, Entrepreneurship & Market Uptake": {
        "sme": ["sme", "small business", "small and medium", "startup", "scaleup"],
        "startup": ["startup", "spin-off", "venture", "new business"],
        "scaleup": ["scale-up", "scaleup", "growth company", "go-to-market"],
        "market_uptake": ["market uptake", "commercialisation", "commercialization", "deployment"],
        "entrepreneurship": ["entrepreneurship", "entrepreneur", "business model"],
    },
    "External Action & International Cooperation": {
        "development": ["development cooperation", "development", "capacity building"],
        "global_health": ["global health", "health systems strengthening"],
        "governance": ["governance", "institutional reform", "public administration"],
        "humanitarian": ["humanitarian", "humanitarian aid", "relief"],
        "international_partnerships": ["international cooperation", "partnership", "bilateral", "multilateral"],
    },
    "Climate-neutral & Smart Cities": {
        "cities": ["city", "cities", "urban", "smart city", "smart cities"],
        "smart_cities": ["smart city", "smart cities", "urban innovation"],
        "urban_mobility": ["urban mobility", "public transport", "shared mobility"],
        "urban_energy": ["district energy", "urban energy", "local energy system"],
        "buildings": ["building", "buildings", "renovation", "energy efficient building"],
        "climate_neutrality": ["climate-neutral", "climate neutral", "net zero city", "urban decarbonisation"],
    },
    "Healthy Oceans, Seas, Coastal & Inland Waters": {
        "marine": ["marine", "ocean", "sea", "maritime ecosystem"],
        "coastal": ["coastal", "shoreline", "coast"],
        "inland_waters": ["lake", "river", "inland waters", "freshwater"],
        "blue_economy": ["blue economy", "fisheries", "aquaculture", "marine biotechnology"],
        "pollution_water": ["water pollution", "marine litter", "microplastic", "wastewater"],
    },
    "Clean Aviation": {
        "aircraft": ["aircraft", "airframe", "aerostructure"],
        "propulsion": ["propulsion", "engine", "hybrid-electric propulsion", "electric propulsion"],
        "sustainable_fuels": ["sustainable aviation fuel", "saf", "aviation fuel"],
        "emissions": ["aviation emissions", "low-emission aviation", "zero-emission aircraft"],
        "airport_ops": ["airport", "ground operation", "air traffic"],
    },
    "Research, Academia": {
        "research_infrastructures": ["research infrastructure", "research infrastructures", "infrastructure", "infrastructures"],
        "open_science": ["eosc", "open science", "open access", "open data"],
        "academia": ["university", "universities", "academic", "academia", "research organisation"],
    },
    "Cross-cutting / Other": {
        "interdisciplinary": ["interdisciplinary", "cross-sector", "cross-cutting"],
        "policy": ["policy", "regulation", "standardisation", "standardization"],
        "skills": ["skills", "training", "upskilling", "reskilling"],
        "digital_transition": ["digital transition", "digital transformation"],
        "green_transition": ["green transition", "sustainability transition"],
    },
}


def clean(value: Any) -> Optional[str]:
    if value is None:
        return None
    text = re.sub(r"\s+", " ", str(value)).strip()
    return text or None


def clean_fulltext(text: str) -> str:
    if not text:
        return ""
    out = str(text).replace("\xa0", " ")
    for pat in NOISE_PATTERNS:
        out = re.sub(pat, " ", out, flags=re.IGNORECASE)
    out = re.sub(r"\s+", " ", out).strip()
    return out


def pick(rx: re.Pattern, text: str) -> Optional[str]:
    match = rx.search(text or "")
    return clean(match.group(1)) if match else None


def parse_date_iso(value: str) -> str:
    s = re.sub(r"\s+", " ", str(value or "")).strip()
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
        month = MONTHS.get(m.group(2).lower())
        if month:
            try:
                return datetime(int(m.group(3)), month, int(m.group(1))).strftime("%Y-%m-%d")
            except ValueError:
                pass

    return ""


def normalize_action(value: str) -> str:
    s = (value or "").lower()
    if "research and innovation action" in s:
        return "RIA"
    if "innovation action" in s:
        return "IA"
    if "coordination and support" in s:
        return "CSA"
    if "cofund" in s:
        return "COFUND"
    return clean(value) or ""


def prog_thematic(programme: str) -> str:
    pl = (programme or "").lower()
    for key, label in PROGRAMME_THEMATIC_MAP:
        if key.lower() in pl:
            return label
    return ""


def name_classify(name: str) -> str:
    name_up = (name or "").upper()
    for keyword, thematic in NAME_THEMATIC_RULES:
        if keyword.upper() in name_up:
            return thematic
    return ""


def beneficiary_hint(action: str, programme: str) -> List[str]:
    a = (action or "").upper()
    p = (programme or "").lower()

    hints: List[str] = []

    if a == "IA":
        hints.extend(["SME", "Large enterprise", "Research organisation"])
    elif a == "RIA":
        hints.extend(["Research organisation", "SME", "Large enterprise"])
    elif a == "CSA":
        hints.extend(["Research organisation", "Public body", "NGO", "SME"])

    if "external action" in p:
        hints.extend(["NGO", "Public body", "Research organisation"])

    seen = set()
    unique = []
    for h in hints:
        if h not in seen:
            unique.append(h)
            seen.add(h)
    return unique


def _topic_id(url: str) -> str:
    s = (url or "").upper().split("?")[0]
    for marker in ["/TOPIC-DETAILS/", "/COMPETITIVE-CALLS-CS/", "/PROSPECT-DETAILS/"]:
        idx = s.find(marker)
        if idx >= 0:
            return s[idx + len(marker):]
    return s


def url_classify(url: str) -> Tuple[str, str, str]:
    tid = _topic_id(url)
    for prefix, subcode, cluster_num, cluster_label, thematic in URL_RULES:
        if prefix not in tid:
            continue
        if subcode is not None and subcode not in tid:
            continue
        return cluster_num, cluster_label, thematic
    return "", "", ""


def detect_cluster_from_topic_code(value: str) -> str:
    match = RE_CLUSTER.search(value or "")
    return match.group(1) if match else ""


def find_manual_override(text: str) -> Optional[Dict[str, str]]:
    up = (text or "").upper()
    for rule in MANUAL_OVERRIDES:
        if rule["match"].upper() in up:
            return rule
    return None


def build_search_blob(parts: List[str]) -> str:
    return re.sub(r"\s+", " ", " ".join([p for p in parts if p])).strip().lower()


def infer_subtopics(thematic_cluster: str, search_blob: str, manual_subtopic: str = "") -> List[str]:
    catalog = TOPIC_CATALOG.get(thematic_cluster, TOPIC_CATALOG["Cross-cutting / Other"])
    text = (search_blob or "").lower()
    found: List[str] = []

    if manual_subtopic and manual_subtopic in catalog:
        found.append(manual_subtopic)

    for key, keywords in catalog.items():
        if any(k.lower() in text for k in keywords):
            if key not in found:
                found.append(key)

    return found


def accept_cookies(page: Page) -> None:
    for label in ["Accept all", "Accept All", "Accept", "I accept", "Agree", "OK"]:
        for scope in [page] + list(page.frames):
            try:
                btn = scope.get_by_role("button", name=re.compile(label, re.IGNORECASE))
                if btn.count():
                    btn.first.click(timeout=2000)
                    page.wait_for_timeout(800)
                    return
            except Exception:
                pass


def wait_cookie_gone(page: Page, max_ms: int = 12000) -> None:
    t0 = time.time()
    while (time.time() - t0) * 1000 < max_ms:
        try:
            body = page.locator("body").inner_text()
        except Exception:
            body = ""
        if COOKIE_TEXT.lower() not in body.lower():
            return
        page.wait_for_timeout(600)


def count_links(page: Page) -> int:
    return page.locator(LINK_SELECTOR).count()


def read_total(page: Page) -> Optional[int]:
    text = page.locator("body").inner_text()
    match = RE_TOTAL.search(text or "")
    return int(match.group(1)) if match else None


def scroll_until(page: Page, expected: int, max_ms: int = 50000) -> int:
    start = time.time()
    last_count = -1
    stable_since = time.time()

    while count_links(page) == 0 and (time.time() - start) * 1000 < 10000:
        accept_cookies(page)
        wait_cookie_gone(page, 3000)
        page.wait_for_timeout(700)

    container = page.evaluate_handle(
        f"""() => {{
            const sel = `{LINK_SELECTOR}`;
            const links = document.querySelectorAll(sel);
            if (!links.length) return null;
            let el = links[0];
            for (let i = 0; i < 20; i++) {{
                if (!el) break;
                const st = window.getComputedStyle(el);
                const oy = st.overflowY;
                if ((oy === 'auto' || oy === 'scroll') && el.scrollHeight > el.clientHeight + 5) return el;
                el = el.parentElement;
            }}
            return null;
        }}"""
    )

    while (time.time() - start) * 1000 < max_ms:
        accept_cookies(page)
        wait_cookie_gone(page, 3000)

        current = count_links(page)
        if current >= expected:
            return current

        if current != last_count:
            last_count = current
            stable_since = time.time()

        try:
            if container:
                page.evaluate("(el) => { el.scrollTop = el.scrollTop + el.clientHeight * 0.9; }", container)
            else:
                page.mouse.wheel(0, 1800)
        except Exception:
            pass

        page.wait_for_timeout(600)

        if time.time() - stable_since > 5:
            try:
                if container:
                    page.evaluate("(el) => { el.scrollTop = el.scrollHeight; }", container)
                else:
                    page.mouse.wheel(0, 5000)
            except Exception:
                pass
            page.wait_for_timeout(600)

    return count_links(page)


def extract_links(page: Page) -> List[str]:
    hrefs = page.evaluate(
        f"""
        () => Array.from(document.querySelectorAll('{LINK_SELECTOR}'))
              .map(a => a.getAttribute('href'))
        """
    )

    out: List[str] = []
    seen = set()

    for href in hrefs or []:
        if not href:
            continue
        full = "https://ec.europa.eu" + href if href.startswith("/") else href
        if full not in seen:
            seen.add(full)
            out.append(full)

    return out


def extract_detail_text(page: Page) -> str:
    texts: List[str] = []

    for sel in DETAIL_TEXT_SELECTORS:
        try:
            loc = page.locator(sel).first
            if loc.count():
                txt = clean_fulltext(loc.inner_text(timeout=3000))
                if txt and len(txt) > 300:
                    texts.append(txt)
        except Exception:
            pass

    if not texts:
        try:
            texts.append(clean_fulltext(page.locator("body").inner_text(timeout=3000)))
        except Exception:
            return ""

    best = max(texts, key=len) if texts else ""
    return best[:50000]


def parse_card(page: Page, full_url: str) -> Dict[str, Any]:
    path = full_url.replace("https://ec.europa.eu", "").split("?")[0]
    anchor = page.locator(f'a[href*="{path}"]').first

    title = clean(anchor.inner_text()) if anchor.count() else path.split("/")[-1]

    card = anchor.locator(
        "xpath=ancestor::*[contains(.,'Programme:') or contains(.,'Opening date:') or contains(.,'Deadline date:') or contains(.,'Type of action:')][1]"
    ).first

    if card.count():
        text = card.inner_text()
    elif anchor.count():
        text = anchor.locator("xpath=ancestor::*[1]").inner_text()
    else:
        text = ""

    deadline = pick(RE_DEAD, text) or pick(RE_NEXT_DEAD, text)
    call_id = pick(RE_CALL_ID, full_url) or pick(RE_CALL_ID, text)
    cluster_raw = pick(RE_CLUSTER, text) or pick(RE_CLUSTER, full_url) or pick(RE_CLUSTER, call_id or "")

    return {
        "name": title or "",
        "call_id": call_id or "",
        "programme_raw": pick(RE_PROG, text) or "",
        "action_raw": pick(RE_ACTION, text) or "",
        "cluster_raw": cluster_raw or "",
        "opening_raw": pick(RE_OPEN, text) or "",
        "deadline_raw": deadline or "",
        "url": full_url,
        "fulltext_raw": "",
    }


def _first(meta: Dict[str, Any], *keys: str) -> str:
    for key in keys:
        value = meta.get(key)
        if isinstance(value, list) and value:
            return re.sub(r"\s+", " ", str(value[0])).strip()
        if isinstance(value, str) and value.strip():
            return value.strip()
    return ""


def enrich_one(page: Page, row: Dict[str, Any]) -> bool:
    url = row["url"]
    captured: Dict[str, str] = {}

    def handle(response) -> None:
        if SEARCH_API_PART in response.url and response.status == 200:
            try:
                body = response.json()
                items = body.get("results", [body])
                for item in items:
                    meta = item.get("metadata", {}) or {}
                    prog_id = _first(meta, "frameworkProgramme", "programme")
                    action = _first(meta, "typesOfAction", "typeOfAction", "fundingScheme")
                    cid = _first(meta, "callIdentifier", "identifier")

                    if prog_id and not captured.get("programme"):
                        captured["programme"] = PROGRAMME_MAP.get(prog_id, prog_id)
                    if action and not captured.get("action"):
                        captured["action"] = action
                    if cid and not captured.get("call_id"):
                        captured["call_id"] = cid
            except Exception:
                pass

    page.on("response", handle)

    try:
        page.goto(url, wait_until="domcontentloaded", timeout=30000)
        page.wait_for_timeout(2500)
        accept_cookies(page)
        wait_cookie_gone(page, 3000)
        page.wait_for_timeout(1000)
        row["fulltext_raw"] = extract_detail_text(page)
    except Exception as exc:
        print(f"    [ERR goto] {exc}", flush=True)
    finally:
        page.remove_listener("response", handle)

    if captured.get("programme") and not row.get("programme_raw"):
        row["programme_raw"] = captured["programme"]
    if captured.get("action") and not row.get("action_raw"):
        row["action_raw"] = captured["action"]
    if captured.get("call_id") and not row.get("call_id"):
        row["call_id"] = captured["call_id"]

    return bool(captured) or bool(row.get("fulltext_raw"))


def enrich_rows(ctx: BrowserContext, rows: List[Dict[str, Any]]) -> None:
    to_fix = [r for r in rows if r.get("url")]
    if not to_fix:
        print("Nessuna call da arricchire.", flush=True)
        return

    print(f"Arricchimento metadata + full text per {len(to_fix)} call…", flush=True)
    page = ctx.new_page()
    skipped = 0

    for idx, row in enumerate(to_fix, start=1):
        print(f"[{idx:>4}/{len(to_fix)}] {(row.get('name') or '')[:80]}", flush=True)
        ok = False

        for attempt in range(1, 3):
            try:
                ok = enrich_one(page, row)
                break
            except Exception as exc:
                print(f"    [tentativo {attempt} fallito] {exc}", flush=True)
                try:
                    page.close()
                except Exception:
                    pass
                page = ctx.new_page()
                time.sleep(2)

        if not ok:
            skipped += 1
            print("    [SKIP] nessun dato recuperato", flush=True)

        if idx % 100 == 0:
            print(f"[checkpoint] processate {idx} call", flush=True)

        time.sleep(0.25)

    try:
        page.close()
    except Exception:
        pass

    print(f"Arricchimento completato. Saltate: {skipped}/{len(to_fix)}", flush=True)


def build_call(row: Dict[str, Any]) -> Dict[str, Any]:
    url = row.get("url", "")
    name = row.get("name", "")
    programme_raw = row.get("programme_raw", "") or ""
    action_raw = row.get("action_raw", "") or ""
    call_id = row.get("call_id", "") or ""
    fulltext = clean_fulltext(row.get("fulltext_raw", "") or "")
    opening_raw = row.get("opening_raw", "") or ""
    deadline_raw = row.get("deadline_raw", "") or ""

    search_upper = " ".join([
        name or "",
        call_id or "",
        url or "",
        programme_raw or "",
        fulltext or "",
    ]).upper()

    manual = find_manual_override(search_upper)

    cluster_num = ""
    for source in [call_id, row.get("cluster_raw", ""), url]:
        cnum = detect_cluster_from_topic_code(source or "")
        if cnum:
            cluster_num = cnum
            break

    url_cluster_num, url_cluster_label, url_thematic = url_classify(url)
    if url_cluster_num:
        cluster_num = url_cluster_num

    cluster_label = url_cluster_label or THEMATIC_MAP.get(cluster_num, "")
    thematic_cluster = (
        manual["cluster"] if manual
        else url_thematic
        or THEMATIC_MAP.get(cluster_num, "")
        or name_classify(name)
        or prog_thematic(programme_raw)
        or "Cross-cutting / Other"
    )

    action = normalize_action(action_raw)
    is_mission = "/HORIZON-MISS" in url.upper()

    search_blob = build_search_blob([
        name,
        call_id,
        programme_raw,
        cluster_label,
        thematic_cluster,
        action,
        opening_raw,
        deadline_raw,
        url,
        fulltext,
    ])

    subtopics = infer_subtopics(
        thematic_cluster=thematic_cluster,
        search_blob=search_blob,
        manual_subtopic=(manual["subtopic"] if manual else ""),
    )

    return {
        "name": name,
        "call_id": call_id,
        "programme": programme_raw,
        "cluster_num": cluster_num,
        "cluster_label": cluster_label,
        "thematic_cluster": thematic_cluster,
        "subtopics": subtopics,
        "action": action,
        "opening": opening_raw,
        "opening_iso": parse_date_iso(opening_raw),
        "deadline": deadline_raw,
        "deadline_iso": parse_date_iso(deadline_raw),
        "url": url,
        "is_mission": is_mission,
        "beneficiary_hint": beneficiary_hint(action, programme_raw),
        "fulltext": fulltext,
        "search_blob": search_blob,
        "manual_override": bool(manual),
        "manual_override_source": manual["match"] if manual else "",
    }


def write_changelog(old_calls: List[Dict[str, Any]], new_calls: List[Dict[str, Any]], changelog_path: Path, generated: str) -> None:
    old_by_url = {c["url"]: c for c in old_calls if c.get("url")}
    new_by_url = {c["url"]: c for c in new_calls if c.get("url")}

    old_urls = set(old_by_url)
    new_urls = set(new_by_url)

    added = [new_by_url[u] for u in sorted(new_urls - old_urls)]
    removed = [old_by_url[u] for u in sorted(old_urls - new_urls)]

    def thematic_counts(calls: List[Dict[str, Any]]) -> Dict[str, int]:
        counts: Dict[str, int] = {}
        for call in calls:
            key = call.get("thematic_cluster") or "(non classificato)"
            counts[key] = counts.get(key, 0) + 1
        return counts

    date_str = generated[:10]

    lines: List[str] = []
    lines.append("# Changelog calls.json")
    lines.append("")
    lines.append(f"**Ultimo aggiornamento:** {generated.replace('T', ' ').replace('+00:00', ' UTC')[:22]}")
    lines.append("")
    lines.append("## Riepilogo")
    lines.append("")
    lines.append("| | Numero |")
    lines.append("|---|---|")
    lines.append(f"| Call totali (nuovo) | {len(new_calls)} |")
    lines.append(f"| Call totali (precedente) | {len(old_calls)} |")
    lines.append(f"| **Nuove call aggiunte** | **{len(added)}** |")
    lines.append(f"| Call rimosse (scadute/chiuse) | {len(removed)} |")
    lines.append("")

    if added:
        lines.append(f"## Call aggiunte ({len(added)})")
        lines.append("")
        by_thematic: Dict[str, List[Dict[str, Any]]] = {}
        for c in added:
            key = c.get("thematic_cluster") or "(non classificato)"
            by_thematic.setdefault(key, []).append(c)

        for thematic, calls in sorted(by_thematic.items()):
            lines.append(f"### {thematic} ({len(calls)})")
            lines.append("")
            for c in calls:
                name = c.get("name") or "(senza nome)"
                prog = c.get("programme") or ""
                action = c.get("action") or ""
                dead = c.get("deadline") or ""
                url = c.get("url") or ""
                meta = " · ".join(filter(None, [prog, action, f"Scadenza: {dead}" if dead else ""]))
                lines.append(f"- **{name}**")
                if meta:
                    lines.append(f"  {meta}")
                if url:
                    lines.append(f"  {url}")
                lines.append("")
    else:
        lines.append("## Call aggiunte")
        lines.append("")
        lines.append("Nessuna nuova call rispetto alla rilevazione precedente.")
        lines.append("")

    if removed:
        lines.append(f"## Call rimosse ({len(removed)})")
        lines.append("")
        for c in removed:
            name = c.get("name") or "(senza nome)"
            prog = c.get("programme") or ""
            dead = c.get("deadline") or ""
            meta = " · ".join(filter(None, [prog, f"Scadenza: {dead}" if dead else ""]))
            lines.append(f"- **{name}**{(' — ' + meta) if meta else ''}")
        lines.append("")

    lines.append("## Distribuzione per area tematica (nuovo dataset)")
    lines.append("")
    lines.append("| Area tematica | Call |")
    lines.append("|---|---|")
    for key, value in sorted(thematic_counts(new_calls).items(), key=lambda x: -x[1]):
        lines.append(f"| {key} | {value} |")
    lines.append("")

    changelog_path.write_text("\n".join(lines), encoding="utf-8")
    print(f"Changelog scritto: {changelog_path} (+{len(added)} / -{len(removed)})")

    history_path = changelog_path.parent / "changelog_history.md"
    history_line = f"| {date_str} | {len(new_calls)} | +{len(added)} | -{len(removed)} |"

    if history_path.exists():
        hist = history_path.read_text(encoding="utf-8")
        if history_line not in hist:
            hist = hist.rstrip() + "\n" + history_line + "\n"
            history_path.write_text(hist, encoding="utf-8")
    else:
        header = (
            "# Storico aggiornamenti calls.json\n\n"
            "| Data | Call totali | Aggiunte | Rimosse |\n"
            "|---|---|---|---|\n"
            f"{history_line}\n"
        )
        history_path.write_text(header, encoding="utf-8")


def main(out_path: Path) -> None:
    rows: List[Dict[str, Any]] = []
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

        page.goto(LIST_URL.format(page=1, ps=PAGE_SIZE), wait_until="domcontentloaded", timeout=90000)
        page.wait_for_timeout(1500)
        accept_cookies(page)
        wait_cookie_gone(page)

        total = read_total(page)
        if total is None:
            print("Non riesco a leggere 'item(s) found'.")
            browser.close()
            return

        max_pages = math.ceil(total / PAGE_SIZE)
        print(f"Totale call: {total} | pagine: {max_pages}")

        for page_num in range(1, max_pages + 1):
            remaining = total - (page_num - 1) * PAGE_SIZE
            expected = min(PAGE_SIZE, remaining)
            url = LIST_URL.format(page=page_num, ps=PAGE_SIZE)

            print(f"[pagina {page_num}/{max_pages}] attese ~{expected}", end="", flush=True)
            page.goto(url, wait_until="domcontentloaded", timeout=90000)
            page.wait_for_timeout(1200)
            accept_cookies(page)
            wait_cookie_gone(page)
            scroll_until(page, expected=expected)

            links = extract_links(page)
            new_links = [u for u in links if u not in seen_urls]
            print(f" -> trovati {len(new_links)} nuovi", flush=True)

            for u in new_links:
                seen_urls.add(u)
                rows.append(parse_card(page, u))

            time.sleep(0.1)

        print(f"\nPasso 2: arricchimento {len(rows)} call", flush=True)
        enrich_rows(ctx, rows)
        browser.close()

    calls: List[Dict[str, Any]] = []
    seen_final = set()

    for row in rows:
        call = build_call(row)
        if call["url"] and call["url"] not in seen_final:
            seen_final.add(call["url"])
            calls.append(call)

    thematic_counts: Dict[str, int] = {}
    with_fulltext = 0

    for c in calls:
        key = c.get("thematic_cluster") or "(non classificato)"
        thematic_counts[key] = thematic_counts.get(key, 0) + 1
        if c.get("fulltext"):
            with_fulltext += 1

    print(f"\nClassificazione ({len(calls)} call):")
    for key, value in sorted(thematic_counts.items(), key=lambda x: -x[1]):
        print(f"  {value:5d}  {key}")

    print(f"\nCall con full text: {with_fulltext}/{len(calls)}")

    generated = datetime.now(timezone.utc).isoformat()

    old_calls: List[Dict[str, Any]] = []
    if out_path.exists():
        try:
            old_data = json.loads(out_path.read_text(encoding="utf-8"))
            old_calls = old_data.get("calls", [])
            print(f"\nDataset precedente: {len(old_calls)} call")
        except Exception:
            print("\nNessun dataset precedente leggibile.")

    changelog_path = out_path.parent / "changelog.md"
    write_changelog(old_calls, calls, changelog_path, generated)

    payload = {
        "generated": generated,
        "calls": calls,
    }

    out_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"\nScritto {out_path} con {len(calls)} call")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--out", default="calls.json", help="Percorso output JSON")
    args = parser.parse_args()
    main(Path(args.out))
