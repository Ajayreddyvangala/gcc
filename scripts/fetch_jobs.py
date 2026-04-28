"""
GCC Intelligence Hub — Nightly Job Fetcher
Runs via GitHub Actions every night at midnight IST
8 JSearch calls (4 queries x 2 cities) → Supabase
"""

import os, re, time, requests
from datetime import datetime, timezone
from supabase import create_client

# ── CREDENTIALS (injected by GitHub Actions secrets) ──────────
SUPABASE_URL = os.environ['SUPABASE_URL']
SUPABASE_KEY = os.environ['SUPABASE_KEY']
JSEARCH_KEY  = os.environ['JSEARCH_KEY']

# ── JSEARCH CONFIG ────────────────────────────────────────────
JSEARCH_URL  = "https://jsearch.p.rapidapi.com/search"
JSEARCH_HOST = "jsearch.p.rapidapi.com"

# 4 targeted queries — city appended per run
QUERIES = [
    "information security manager OR lead OR director OR engineer OR analyst OR architect",
    "cybersecurity manager OR lead OR director OR head OR architect OR VP",
    "identity access management IAM manager OR lead OR director OR engineer OR architect",
    "PING azure active directory security GCP endpoint security manager OR lead OR engineer",
]

CITIES = [
    {"key": "HYD", "name": "Hyderabad"},
    {"key": "BLR", "name": "Bengaluru"},
]

# ── GCC MASTER LIST WITH IDs ──────────────────────────────────
# Used to match jobs back to the right tile
GCC_LIST = [
    # HYD
    (1,  "Microsoft",              "HYD"),
    (2,  "Google",                 "HYD"),
    (3,  "Amazon",                 "HYD"),
    (4,  "JP Morgan Chase",        "HYD"),
    (5,  "HSBC",                   "HYD"),
    (6,  "Goldman Sachs",          "HYD"),
    (7,  "Wells Fargo",            "HYD"),
    (8,  "Ping Identity",          "HYD"),
    (9,  "SailPoint",              "HYD"),
    (10, "MetLife",                "HYD"),
    (11, "Vanguard",               "HYD"),
    (12, "Salesforce",             "HYD"),
    (13, "IBM",                    "HYD"),
    (14, "Palo Alto Networks",     "HYD"),
    (15, "CrowdStrike",            "HYD"),
    (16, "Oracle",                 "HYD"),
    (17, "SAP",                    "HYD"),
    (18, "Cisco",                  "HYD"),
    (19, "Accenture",              "HYD"),
    (20, "Deloitte",               "HYD"),
    (21, "PwC",                    "HYD"),
    (22, "EY",                     "HYD"),
    (23, "KPMG",                   "HYD"),
    (24, "Capgemini",              "HYD"),
    (25, "Wipro",                  "HYD"),
    (26, "Infosys",                "HYD"),
    (27, "TCS",                    "HYD"),
    (28, "HCL Technologies",       "HYD"),
    (29, "Cognizant",              "HYD"),
    (30, "Novartis",               "HYD"),
    (31, "AstraZeneca",            "HYD"),
    (32, "Sanofi",                 "HYD"),
    (33, "Eli Lilly",              "HYD"),
    (34, "HCA Healthcare",         "HYD"),
    (35, "McDonald's",             "HYD"),
    (36, "Walmart",                "HYD"),
    (37, "Target",                 "HYD"),
    (38, "Deutsche Bank",          "HYD"),
    (39, "Barclays",               "HYD"),
    (40, "Citi",                   "HYD"),
    (41, "Shell",                  "HYD"),
    (42, "ABB",                    "HYD"),
    (43, "Honeywell",              "HYD"),
    (44, "Swiss Re",               "HYD"),
    (45, "Franklin Templeton",     "HYD"),
    (46, "BeyondTrust",            "HYD"),
    (47, "Secureworks",            "HYD"),
    (48, "Sonatype",               "HYD"),
    (49, "T-Mobile",               "HYD"),
    (50, "Tesco",                  "HYD"),
    (51, "State Street",           "HYD"),
    (52, "Bosch",                  "HYD"),
    (53, "Qualcomm",               "HYD"),
    (54, "Marriott",               "HYD"),
    (55, "BP",                     "HYD"),
    (56, "Roche",                  "HYD"),
    (57, "ServiceNow",             "HYD"),
    (58, "Adobe",                  "HYD"),
    (59, "Nvidia",                 "HYD"),
    (60, "Citi Treasury",          "HYD"),
    (61, "Fortinet",               "HYD"),
    (62, "VMware",                 "HYD"),
    (63, "Hitachi Vantara",        "HYD"),
    (64, "Micron Technology",      "HYD"),
    (65, "NICE Systems",           "HYD"),
    (66, "ManTech",                "HYD"),
    (67, "Lonza",                  "HYD"),
    (68, "DAZN",                   "HYD"),
    (69, "Pegasystems",            "HYD"),
    (70, "ArcelorMittal",          "HYD"),
    # BLR
    (101, "Microsoft",             "BLR"),
    (102, "Google",                "BLR"),
    (103, "Amazon",                "BLR"),
    (104, "Goldman Sachs",         "BLR"),
    (105, "Morgan Stanley",        "BLR"),
    (106, "JP Morgan Chase",       "BLR"),
    (107, "Walmart",               "BLR"),
    (108, "Target",                "BLR"),
    (109, "Flipkart",              "BLR"),
    (110, "Infosys",               "BLR"),
    (111, "Wipro",                 "BLR"),
    (112, "TCS",                   "BLR"),
    (113, "HCL Technologies",      "BLR"),
    (114, "Accenture",             "BLR"),
    (115, "IBM",                   "BLR"),
    (116, "SAP",                   "BLR"),
    (117, "Oracle",                "BLR"),
    (118, "Cisco",                 "BLR"),
    (119, "Deloitte",              "BLR"),
    (120, "PwC",                   "BLR"),
    (121, "EY",                    "BLR"),
    (122, "KPMG",                  "BLR"),
    (123, "Bosch",                 "BLR"),
    (124, "Siemens",               "BLR"),
    (125, "ABB",                   "BLR"),
    (126, "Honeywell",             "BLR"),
    (127, "GE",                    "BLR"),
    (128, "Boeing",                "BLR"),
    (129, "Airbus",                "BLR"),
    (130, "AstraZeneca",           "BLR"),
    (131, "Novartis",              "BLR"),
    (132, "Philips",               "BLR"),
    (133, "Medtronic",             "BLR"),
    (134, "HSBC",                  "BLR"),
    (135, "Deutsche Bank",         "BLR"),
    (136, "Barclays",              "BLR"),
    (137, "Standard Chartered",    "BLR"),
    (138, "Citi",                  "BLR"),
    (139, "UBS",                   "BLR"),
    (140, "BlackRock",             "BLR"),
    (141, "Fidelity",              "BLR"),
    (142, "Visa",                  "BLR"),
    (143, "Mastercard",            "BLR"),
    (144, "PayPal",                "BLR"),
    (145, "Salesforce",            "BLR"),
    (146, "Adobe",                 "BLR"),
    (147, "Capgemini",             "BLR"),
    (148, "Cognizant",             "BLR"),
    (149, "ServiceNow",            "BLR"),
    (150, "Palo Alto Networks",    "BLR"),
    (151, "CrowdStrike",           "BLR"),
    (152, "VMware",                "BLR"),
    (153, "Texas Instruments",     "BLR"),
    (154, "Intel",                 "BLR"),
    (155, "Unilever",              "BLR"),
    (156, "P&G",                   "BLR"),
    (157, "Shell",                 "BLR"),
    (158, "McKinsey",              "BLR"),
    (159, "BCG",                   "BLR"),
    (160, "Mphasis",               "BLR"),
    (161, "LTIMindtree",           "BLR"),
    (162, "Fiserv",                "BLR"),
    (163, "State Street",          "BLR"),
    (164, "Caterpillar",           "BLR"),
    (165, "Volvo",                 "BLR"),
    (166, "Rockwell Automation",   "BLR"),
    (167, "Sanofi",                "BLR"),
    (168, "Zebra Technologies",    "BLR"),
    (169, "BP",                    "BLR"),
    (170, "Concentrix",            "BLR"),
]

# ── COMPANY NAME ALIASES ──────────────────────────────────────
ALIASES = {
    "amazon":           ["amazon", "aws", "amazon web services", "amazon dev"],
    "jp morgan":        ["jpmorgan", "jp morgan", "j.p. morgan", "jpmc"],
    "vmware":           ["vmware", "broadcom"],
    "p&g":              ["procter", "gamble", "p&g"],
    "ge":               ["general electric", "ge aviation", "ge digital"],
    "bcg":              ["boston consulting", "bcg"],
    "hcl":              ["hcl", "hcltech"],
    "ltimindtree":      ["ltimindtree", "lti mindtree", "mindtree", "larsen toubro"],
    "mckinsey":         ["mckinsey"],
    "walmart":          ["walmart"],
    "mcdonald":         ["mcdonald"],
    "marriott":         ["marriott"],
    "t-mobile":         ["t-mobile", "tmobile"],
    "tesco":            ["tesco"],
    "pwc":              ["pricewaterhousecoopers", "price waterhouse", "pwc"],
    "ey":               ["ernst", "ernst young", "ernst & young"],
    "dazn":             ["dazn"],
    "beyondtrust":      ["beyondtrust", "beyond trust"],
    "secureworks":      ["secureworks"],
    "sailpoint":        ["sailpoint"],
    "palo alto":        ["palo alto networks", "palo alto"],
    "crowdstrike":      ["crowdstrike"],
    "servicenow":       ["servicenow"],
    "astrazeneca":      ["astrazeneca", "astra zeneca"],
    "standard chartered": ["standard chartered", "stanchart"],
    "infosys":          ["infosys", "infosys bpm"],
    "texas instruments": ["texas instruments", "ti "],
    "ping identity":    ["ping identity", "pingidentity", "forgerock"],
}


def norm(s):
    """Normalise company name for matching."""
    s = s.lower()
    s = re.sub(r'[^a-z0-9 ]', ' ', s)
    s = re.sub(r'\b(inc|llc|ltd|limited|corp|corporation|group|india|pvt|'
               r'private|co|company|the|and|solutions|services|technology|'
               r'technologies|global|tech|bank|financial|consulting)\b', ' ', s)
    return re.sub(r'\s+', ' ', s).strip()


def get_variants(name):
    """Return all name variants for matching."""
    variants = {norm(name)}
    nl = name.lower()
    for key, alts in ALIASES.items():
        if key in nl:
            variants.update(alts)
    words = norm(name).split()
    if words:
        variants.add(words[0])
    return variants


def match_gcc(employer_name, city_key):
    """Find GCC id for a given employer name and city."""
    if not employer_name:
        return None
    en = norm(employer_name)
    for gcc_id, gcc_name, gcc_city in GCC_LIST:
        if gcc_city != city_key:
            continue
        for v in get_variants(gcc_name):
            if en == v or en.startswith(v) or v in en:
                return gcc_id
    return None


def fetch_jsearch(query, city_name):
    """Call JSearch API and return list of job dicts."""
    full_query = f"{query} in {city_name} India"
    headers = {
        "X-RapidAPI-Key":  JSEARCH_KEY,
        "X-RapidAPI-Host": JSEARCH_HOST,
    }
    params = {
        "query":            full_query,
        "num_pages":        "1",
        "date_posted":      "month",
        "employment_types": "FULLTIME",
        "job_requirements": "no_degree",
    }
    try:
        r = requests.get(JSEARCH_URL, headers=headers, params=params, timeout=30)
        r.raise_for_status()
        return r.json().get("data", [])
    except Exception as e:
        print(f"  JSearch error for '{full_query}': {e}")
        return []


def main():
    print(f"\n{'='*60}")
    print(f"GCC Job Fetcher — {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}")
    print(f"{'='*60}")

    sb = create_client(SUPABASE_URL, SUPABASE_KEY)

    # ── Mark all existing jobs inactive (will re-activate if still found)
    sb.table("gcc_jobs").update({"is_active": False}).neq("id", "x").execute()
    print("Marked existing jobs inactive")

    all_jobs   = []
    seen_ids   = set()
    call_count = 0

    # ── 8 JSearch calls: 4 queries × 2 cities ─────────────────
    for city in CITIES:
        print(f"\n📍 Fetching for {city['name']}...")
        for q in QUERIES:
            print(f"  Query: {q[:55]}…")
            results = fetch_jsearch(q, city["name"])
            call_count += 1
            matched = 0

            for job in results:
                job_id = job.get("job_id", "")
                if not job_id or job_id in seen_ids:
                    continue
                seen_ids.add(job_id)

                employer = job.get("employer_name", "")
                gcc_id   = match_gcc(employer, city["key"])
                if not gcc_id:
                    continue

                matched += 1
                all_jobs.append({
                    "id":          job_id,
                    "gcc_id":      gcc_id,
                    "company":     employer,
                    "city":        city["key"],
                    "title":       job.get("job_title", ""),
                    "location":    job.get("job_city", city["name"]),
                    "url":         job.get("job_apply_link", ""),
                    "source":      "jsearch",
                    "posted_date": job.get("job_posted_at_datetime_utc", ""),
                    "fetched_at":  datetime.now(timezone.utc).isoformat(),
                    "is_active":   True,
                })

            print(f"    → {len(results)} jobs returned, {matched} matched to GCCs")
            time.sleep(0.5)   # be polite to the API

    # ── Upsert all matched jobs to Supabase ───────────────────
    print(f"\n💾 Upserting {len(all_jobs)} jobs to Supabase…")
    if all_jobs:
        chunk = 100
        for i in range(0, len(all_jobs), chunk):
            sb.table("gcc_jobs").upsert(
                all_jobs[i:i+chunk],
                on_conflict="id"
            ).execute()
        print(f"  ✓ {len(all_jobs)} jobs saved")

    # ── Save fetch status ─────────────────────────────────────
    companies_with_jobs = len(set(j["gcc_id"] for j in all_jobs))
    sb.table("gcc_fetch_status").insert({
        "fetched_at":           datetime.now(timezone.utc).isoformat(),
        "total_jobs":           len(all_jobs),
        "companies_with_jobs":  companies_with_jobs,
    }).execute()

    print(f"\n{'='*60}")
    print(f"✅ Done — {call_count} API calls, {len(all_jobs)} jobs, "
          f"{companies_with_jobs} companies with openings")
    print(f"{'='*60}\n")


if __name__ == "__main__":
    main()
