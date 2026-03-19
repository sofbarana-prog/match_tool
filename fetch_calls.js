/**
 * fetch_calls.js
 * Runs inside GitHub Actions (no CORS issue).
 * Fetches all open/forthcoming calls from the EU Funding & Tenders API
 * and writes calls.json with the same classification logic as make_calls_json.py.
 */

import { writeFileSync } from "fs";

const EU_API =
  "https://api.tech.ec.europa.eu/search-api/prod/rest/search";
const PAGE_SIZE = 50;

// ─── CLASSIFICATION TABLES ────────────────────────────────────────────────────

const PROGRAMME_MAP = {
  "43108390": "Horizon Europe", "43108391": "Horizon Europe",
  "43152860": "Digital Europe Programme", "111111": "EU External Action",
  "44181033": "European Defence Fund", "43353764": "Erasmus+",
  "43251589": "CERV", "43251814": "Creative Europe (CREA)",
  "43252476": "Single Market Programme (SMP)", "43298664": "AGRIP",
  "43251842": "EUAF", "43298916": "Euratom",
  "43089234": "Innovation Fund (INNOVFUND)", "43637601": "PPPA",
  "44416173": "I3", "45532249": "EUBA",
  "43252368": "Internal Security Fund (ISF)", "43252449": "RFCS",
  "43298203": "UCPM", "43254037": "European Solidarity Corps (ESC)",
  "44773066": "Just Transition Mechanism (JTM)",
  "43251567": "Connecting Europe Facility (CEF)",
  "43252386": "JUST", "43252433": "Pericles IV", "43252517": "SOCPL",
  "43253967": "RENEWFM", "43254019": "European Social Fund+ (ESF+)",
  "43392145": "EMFAF",
};

const THEMATIC_MAP = {
  "1": "Health & Life Sciences",
  "2": "Culture, Creativity & Inclusion",
  "3": "Security & Resilience",
  "4": "Digital, Industry & Space",
  "5": "Climate, Energy & Mobility",
  "6": "Food, Bioeconomy & Environment",
  "M-CIT":   "Climate-neutral & Smart Cities",
  "M-OCEAN": "Healthy Oceans, Seas, Coastal & Inland Waters",
};

const PROGRAMME_THEMATIC_MAP = [
  ["European Defence Fund",           "Defence"],
  ["EDF",                             "Defence"],
  ["EU External Action",              "External Action & International Cooperation"],
  ["EU External Action-Prospect",     "External Action & International Cooperation"],
  ["Single Market Programme",         "SME, Entrepreneurship & Market Uptake"],
  ["CERV",                            "Culture, Creativity & Inclusion"],
  ["Creative Europe",                 "Culture, Creativity & Inclusion"],
  ["Erasmus+",                        "Culture, Creativity & Inclusion"],
  ["European Social Fund+",           "Culture, Creativity & Inclusion"],
  ["Just Transition",                 "Climate, Energy & Mobility"],
  ["Innovation Fund",                 "Climate, Energy & Mobility"],
  ["EMFAF",                           "Food, Bioeconomy & Environment"],
  ["LIFE",                            "Food, Bioeconomy & Environment"],
  ["Euratom",                         "Climate, Energy & Mobility"],
  ["Connecting Europe",               "Climate, Energy & Mobility"],
  ["Internal Security Fund",          "Security & Resilience"],
  ["European Solidarity Corps",       "Culture, Creativity & Inclusion"],
  ["Digital Europe",                  "Digital, Industry & Space"],
  ["RENEWFM",                         "Climate, Energy & Mobility"],
  ["SOCPL",                           "Culture, Creativity & Inclusion"],
  ["JUST",                            "Culture, Creativity & Inclusion"],
  ["Pericles IV",                     "Culture, Creativity & Inclusion"],
  ["I3",                              "SME, Entrepreneurship & Market Uptake"],
  ["ERC",                             "Cross-cutting / Other"],
  ["43392145",                        "Food, Bioeconomy & Environment"],
  ["Horizon Europe",                  "Cross-cutting / Other"],
];

// [prefix, subcode_or_null, thematic, cluster_num_override]
const URL_RULES = [
  ["MISS", "CIT",    "Climate-neutral & Smart Cities",               "M-CIT"],
  ["MISS", "OCEAN",  "Healthy Oceans, Seas, Coastal & Inland Waters","M-OCEAN"],
  ["MISS", "CLIMA",  "Climate, Energy & Mobility",                   "5"],
  ["MISS", "CANCER", "Health & Life Sciences",                       "1"],
  ["MISS", "SOIL",   "Food, Bioeconomy & Environment",               "6"],
  ["MISS", "CROSS",  "Cross-cutting / Other",                        ""],
  ["HLTH",    null,  "Health & Life Sciences",                       "1"],
  ["EIC",     null,  "SME, Entrepreneurship & Market Uptake",        ""],
  ["EIE",     null,  "SME, Entrepreneurship & Market Uptake",        ""],
  ["EIT",     null,  "SME, Entrepreneurship & Market Uptake",        ""],
  ["CID",     null,  "Climate, Energy & Mobility",                   "5"],
  ["EURATOM", null,  "Climate, Energy & Mobility",                   "5"],
  ["EUROHPC", null,  "Digital, Industry & Space",                    "4"],
  ["JU-CLEAN-AVIATION", null, "Clean Aviation",                      ""],
  ["JU-",     null,  "Climate, Energy & Mobility",                   ""],
  ["MSCA",    null,  "Cross-cutting / Other",                        ""],
  ["NEB",     null,  "Climate-neutral & Smart Cities",               ""],
  ["RAISE",   null,  "Cross-cutting / Other",                        ""],
  ["WIDERA",  null,  "Cross-cutting / Other",                        ""],
  ["INFRA",   null,  "Cross-cutting / Other",                        ""],
  ["AGRIP",   null,  "Food, Bioeconomy & Environment",               "6"],
  ["EUAF",    null,  "Cross-cutting / Other",                        ""],
  ["DIGITAL", null,  "Digital, Industry & Space",                    "4"],
  ["UCPM",    null,  "Cross-cutting / Other",                        ""],
  ["RFCS",    null,  "Climate, Energy & Mobility",                   "5"],
  ["EUBA",    null,  "External Action & International Cooperation",  ""],
  ["PPPA",   "CHIPS","Digital, Industry & Space",                    "4"],
  ["PPPA",   "MEDIA","Culture, Creativity & Inclusion",              ""],
  ["PPPA",    null,  "Digital, Industry & Space",                    "4"],
  ["RENEWFM", null,  "Climate, Energy & Mobility",                   "5"],
  ["SOCPL",   null,  "Culture, Creativity & Inclusion",              ""],
  ["ERC",     null,  "Cross-cutting / Other",                        ""],
  ["EMFAF",   null,  "Food, Bioeconomy & Environment",               "6"],
  ["JUST",    null,  "Culture, Creativity & Inclusion",              ""],
  ["I3",      null,  "SME, Entrepreneurship & Market Uptake",        ""],
];

const URL_BENEFICIARY_OVERRIDE = {
  "MSCA":  ["Research organisation"],
  "INFRA": ["Research organisation"],
  "EUAF":  ["Research organisation"],
  "EUBA":  ["Public body"],
};

// ─── HELPERS ─────────────────────────────────────────────────────────────────

function topicId(url) {
  const s = (url || "").toUpperCase().split("?")[0];
  for (const m of ["/TOPIC-DETAILS/", "/COMPETITIVE-CALLS-CS/"]) {
    const i = s.indexOf(m);
    if (i >= 0) return s.slice(i + m.length);
  }
  return s;
}

function urlClassify(url) {
  const tid = topicId(url);
  for (const [prefix, subcode, thematic, clusterOverride] of URL_RULES) {
    if (!tid.includes(prefix)) continue;
    if (subcode !== null) {
      if (!tid.includes(`-${subcode}-`) && !tid.endsWith(`-${subcode}`)) continue;
    }
    return {
      thematic,
      clusterOverride,
      benef: URL_BENEFICIARY_OVERRIDE[prefix] || null,
    };
  }
  return { thematic: "", clusterOverride: "", benef: null };
}

function clusterFromText(s) {
  const m = (s || "").match(/HORIZON-CL([1-6])/i);
  return m ? m[1] : "";
}

function progThematic(prog) {
  const pl = (prog || "").toLowerCase();
  for (const [key, label] of PROGRAMME_THEMATIC_MAP) {
    if (pl.includes(key.toLowerCase())) return label;
  }
  return "";
}

function resolveThematic(clusterNum, prog) {
  if (clusterNum && THEMATIC_MAP[clusterNum]) return THEMATIC_MAP[clusterNum];
  return progThematic(prog);
}

function normalizeAction(v) {
  const s = (v || "").toLowerCase();
  if (s.includes("research and innovation")) return "RIA";
  if (s.includes("innovation action"))       return "IA";
  if (s.includes("coordination"))            return "CSA";
  if (s.includes("cofund"))                  return "COFUND";
  return v || "";
}

function benefHint(action, prog, urlBenef) {
  if (urlBenef) return urlBenef;
  const a = (action || "").toUpperCase();
  const p = (prog  || "").toLowerCase();
  const h = [];
  if (a === "IA")  h.push("SME", "Large enterprise", "Research organisation");
  if (a === "RIA") h.push("Research organisation", "SME", "Large enterprise");
  if (a === "CSA") h.push("Research organisation", "Public body", "NGO", "SME");
  if (p.includes("external action")) h.push("NGO", "Public body", "Research organisation");
  return [...new Set(h)];
}

// ─── TRANSFORM API RESULT → CALL OBJECT ──────────────────────────────────────

function transform(item) {
  const meta       = item.metadata || {};
  const url        = meta.esST_URL || meta.url || "";
  const identifier = meta.identifier || "";
  const progId     = meta.frameworkProgramme || "";
  const prog       = PROGRAMME_MAP[progId] || progId;
  const action     = normalizeAction(meta.typesOfAction || "");

  let clusterNum = clusterFromText(identifier) || clusterFromText(url);

  const { thematic: urlThematic, clusterOverride, benef: urlBenef } = urlClassify(url);
  if (clusterOverride) clusterNum = clusterOverride;

  const thematic   = urlThematic || resolveThematic(clusterNum, prog);
  const isMission  = /\/HORIZON-MISS/i.test(url);
  const deadlineIso = (meta.deadlineDate || "").slice(0, 10);
  const openingIso  = (meta.startDate    || "").slice(0, 10);

  return {
    name:             meta.title || "",
    call_id:          identifier,
    programme:        prog,
    cluster_num:      clusterNum,
    thematic_cluster: thematic,
    action,
    opening:          openingIso,
    opening_iso:      openingIso,
    deadline:         deadlineIso,
    deadline_iso:     deadlineIso,
    url,
    is_mission:       isMission,
    beneficiary_hint: benefHint(action, prog, urlBenef),
  };
}

// ─── FETCH ALL PAGES ──────────────────────────────────────────────────────────

async function fetchPage(page) {
  // The EU API requires browser-like headers — plain requests get 405.
  // Send GET with spoofed Origin/Referer to match what the portal does.
  const url =
    `${EU_API}?apiKey=SEDIA&text=***` +
    `&pageSize=${PAGE_SIZE}&pageNumber=${page}` +
    `&query%2Fstatus%2Fcode=31094501&query%2Fstatus%2Fcode=31094502` +
    `&query%2FprogrammePeriod%2Fcode=2021%20-%202027` +
    `&sortBy=startDate&order=DESC&languages=en`;

  const headers = {
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
    "Origin": "https://ec.europa.eu",
    "Referer": "https://ec.europa.eu/info/funding-tenders/opportunities/portal/screen/opportunities/calls-for-proposals",
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
  };

  let resp = await fetch(url, { headers });

  // Fallback: try POST if GET is blocked
  if (resp.status === 405) {
    console.log(`  GET blocked (405), retrying with POST on page ${page}…`);
    resp = await fetch(EU_API, {
      method: "POST",
      headers: { ...headers, "Content-Type": "application/json" },
      body: JSON.stringify({
        apiKey: "SEDIA",
        text: "***",
        pageSize: PAGE_SIZE,
        pageNumber: page,
        sortBy: "startDate",
        order: "DESC",
        languages: ["en"],
        "query/status/code": ["31094501", "31094502"],
        "query/programmePeriod/code": ["2021 - 2027"],
      }),
    });
  }

  if (!resp.ok) {
    const text = await resp.text().catch(() => "");
    throw new Error(`API error ${resp.status} on page ${page}: ${text.slice(0, 300)}`);
  }
  return resp.json();
}

async function fetchAll() {
  console.log("Fetching page 1…");
  const first = await fetchPage(1);
  const total = first.totalResults || first.total || 0;
  const pages = Math.ceil(total / PAGE_SIZE);
  console.log(`Total: ${total} calls across ${pages} pages`);

  const results = [...(first.results || [])];

  // fetch remaining pages with a small delay to be polite
  for (let p = 2; p <= pages; p++) {
    process.stdout.write(`  page ${p}/${pages}\r`);
    await new Promise(r => setTimeout(r, 200));
    const data = await fetchPage(p);
    results.push(...(data.results || []));
  }

  console.log(`\nFetched ${results.length} raw results`);
  return results;
}

// ─── MAIN ─────────────────────────────────────────────────────────────────────

const raw    = await fetchAll();
const seen   = new Set();
const calls  = [];

for (const item of raw) {
  const call = transform(item);
  if (call.url && !seen.has(call.url)) {
    seen.add(call.url);
    calls.push(call);
  }
}

// Stats
const thematicCounts = {};
for (const c of calls) {
  const k = c.thematic_cluster || "(unclassified)";
  thematicCounts[k] = (thematicCounts[k] || 0) + 1;
}
console.log("\nThematic distribution:");
for (const [k, v] of Object.entries(thematicCounts).sort((a, b) => b[1] - a[1])) {
  console.log(`  ${String(v).padStart(5)}  ${k}`);
}
const unclassified = calls.filter(c => !c.thematic_cluster).length;
console.log(`\nUnclassified: ${unclassified} / ${calls.length}`);

writeFileSync(
  "calls.json",
  JSON.stringify({ generated: new Date().toISOString(), calls }, null, 2),
  "utf-8"
);
console.log(`\nWrote calls.json with ${calls.length} calls`);
