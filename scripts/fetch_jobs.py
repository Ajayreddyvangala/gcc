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
    # HYD — True GCCs only
    (1,  "Microsoft",              "HYD"), (2,  "Google",                "HYD"),
    (3,  "Amazon",                 "HYD"), (4,  "JP Morgan Chase",       "HYD"),
    (5,  "HSBC",                   "HYD"), (6,  "Goldman Sachs",         "HYD"),
    (7,  "Wells Fargo",            "HYD"), (8,  "Ping Identity",         "HYD"),
    (9,  "SailPoint",              "HYD"), (10, "MetLife",               "HYD"),
    (11, "Vanguard",               "HYD"), (12, "Salesforce",            "HYD"),
    (13, "IBM",                    "HYD"), (14, "Palo Alto Networks",    "HYD"),
    (15, "CrowdStrike",            "HYD"), (16, "Oracle",                "HYD"),
    (17, "SAP",                    "HYD"), (18, "Cisco",                 "HYD"),
    (19, "Accenture",              "HYD"), (20, "Deloitte",              "HYD"),
    (21, "PwC",                    "HYD"), (22, "EY",                    "HYD"),
    (23, "KPMG",                   "HYD"), (30, "Novartis",              "HYD"),
    (31, "AstraZeneca",            "HYD"), (32, "Sanofi",                "HYD"),
    (33, "Eli Lilly",              "HYD"), (34, "HCA Healthcare",        "HYD"),
    (35, "McDonald's",             "HYD"), (36, "Walmart",               "HYD"),
    (37, "Target",                 "HYD"), (38, "Deutsche Bank",         "HYD"),
    (39, "Barclays",               "HYD"), (40, "Citi",                  "HYD"),
    (41, "Shell",                  "HYD"), (42, "ABB",                   "HYD"),
    (43, "Honeywell",              "HYD"), (44, "Swiss Re",              "HYD"),
    (45, "Franklin Templeton",     "HYD"), (46, "BeyondTrust",           "HYD"),
    (47, "Secureworks",            "HYD"), (48, "Sonatype",              "HYD"),
    (49, "T-Mobile",               "HYD"), (50, "Tesco",                 "HYD"),
    (51, "State Street",           "HYD"), (52, "Bosch",                 "HYD"),
    (53, "Qualcomm",               "HYD"), (54, "Marriott",              "HYD"),
    (55, "BP",                     "HYD"), (56, "Roche",                 "HYD"),
    (57, "ServiceNow",             "HYD"), (58, "Adobe",                 "HYD"),
    (59, "Nvidia",                 "HYD"), (61, "Fortinet",              "HYD"),
    (62, "VMware",                 "HYD"), (63, "Hitachi Vantara",       "HYD"),
    (64, "Micron Technology",      "HYD"), (65, "NICE Systems",          "HYD"),
    (67, "Lonza",                  "HYD"), (68, "DAZN",                  "HYD"),
    (69, "Pegasystems",            "HYD"), (70, "ArcelorMittal",         "HYD"),
    (71, "AT&T",                   "HYD"), (72, "Amgen",                 "HYD"),
    (73, "Broadridge",             "HYD"), (74, "NatWest Group",         "HYD"),
    (75, "Synchrony Financial",    "HYD"), (76, "Allstate",              "HYD"),
    (77, "Northern Trust",         "HYD"), (78, "Charles Schwab",        "HYD"),
    (79, "Thermo Fisher",          "HYD"), (80, "Danaher",               "HYD"),
    (81, "Sabre Corporation",      "HYD"), (82, "FIS",                   "HYD"),
    (83, "Genpact",                "HYD"), (84, "WEX Inc",               "HYD"),
    (85, "Lam Research",           "HYD"), (86, "Applied Materials",     "HYD"),
    (87, "Synopsys",               "HYD"), (88, "Cadence",               "HYD"),
    (89, "NXP Semiconductors",     "HYD"), (90, "Infineon",              "HYD"),
    (91, "Zurich Insurance",       "HYD"), (92, "Manulife",              "HYD"),
    (93, "Principal Financial",    "HYD"), (94, "Nationwide",            "HYD"),
    (95, "Humana",                 "HYD"), (96, "Elevance Health",       "HYD"),
    (97, "Aptiv",                  "HYD"), (98, "OpenText",              "HYD"),
    (99, "Conduent",               "HYD"),
    # BLR — True GCCs only
    (101,"Microsoft",              "BLR"), (102,"Google",                "BLR"),
    (103,"Amazon",                 "BLR"), (104,"Goldman Sachs",         "BLR"),
    (105,"Morgan Stanley",         "BLR"), (106,"JP Morgan Chase",       "BLR"),
    (107,"Walmart",                "BLR"), (108,"Target",                "BLR"),
    (109,"Flipkart",               "BLR"), (110,"IBM",                   "BLR"),
    (111,"SAP",                    "BLR"), (112,"Oracle",                "BLR"),
    (113,"Cisco",                  "BLR"), (114,"Accenture",             "BLR"),
    (115,"Deloitte",               "BLR"), (116,"PwC",                   "BLR"),
    (117,"EY",                     "BLR"), (118,"KPMG",                  "BLR"),
    (119,"Bosch",                  "BLR"), (120,"Siemens",               "BLR"),
    (121,"ABB",                    "BLR"), (122,"Honeywell",             "BLR"),
    (123,"GE",                     "BLR"), (124,"Boeing",                "BLR"),
    (125,"Airbus",                 "BLR"), (126,"AstraZeneca",           "BLR"),
    (127,"Novartis",               "BLR"), (128,"Philips",               "BLR"),
    (129,"Medtronic",              "BLR"), (130,"HSBC",                  "BLR"),
    (131,"Deutsche Bank",          "BLR"), (132,"Barclays",              "BLR"),
    (133,"Standard Chartered",     "BLR"), (134,"Citi",                  "BLR"),
    (135,"UBS",                    "BLR"), (136,"BlackRock",             "BLR"),
    (137,"Fidelity",               "BLR"), (138,"Visa",                  "BLR"),
    (139,"Mastercard",             "BLR"), (140,"PayPal",                "BLR"),
    (141,"Salesforce",             "BLR"), (142,"Adobe",                 "BLR"),
    (143,"ServiceNow",             "BLR"), (144,"Palo Alto Networks",    "BLR"),
    (145,"CrowdStrike",            "BLR"), (146,"VMware",                "BLR"),
    (147,"Texas Instruments",      "BLR"), (148,"Intel",                 "BLR"),
    (149,"Unilever",               "BLR"), (150,"P&G",                   "BLR"),
    (151,"Shell",                  "BLR"), (152,"McKinsey",              "BLR"),
    (153,"BCG",                    "BLR"), (154,"Fiserv",                "BLR"),
    (155,"State Street",           "BLR"), (156,"Caterpillar",           "BLR"),
    (157,"Volvo",                  "BLR"), (158,"Rockwell Automation",   "BLR"),
    (159,"Sanofi",                 "BLR"), (160,"Zebra Technologies",    "BLR"),
    (161,"BP",                     "BLR"), (171,"AT&T",                  "BLR"),
    (172,"Amgen",                  "BLR"), (173,"Broadridge",            "BLR"),
    (174,"NatWest Group",          "BLR"), (175,"Northern Trust",        "BLR"),
    (176,"Thermo Fisher",          "BLR"), (177,"Danaher",               "BLR"),
    (178,"Aptiv",                  "BLR"), (179,"FIS",                   "BLR"),
    (180,"Genpact",                "BLR"), (181,"Lam Research",          "BLR"),
    (182,"Applied Materials",      "BLR"), (183,"Synopsys",              "BLR"),
    (184,"Cadence",                "BLR"), (185,"NXP Semiconductors",    "BLR"),
    (186,"Infineon",               "BLR"), (187,"Zurich Insurance",      "BLR"),
    (188,"Wells Fargo",            "BLR"), (189,"Synchrony Financial",   "BLR"),
    (190,"Elevance Health",        "BLR"),
]

# ── COMPANY NAME ALIASES ──────────────────────────────────────
ALIASES = {
    # ── Big Tech ─────────────────────────────────────────────
    "microsoft":        ["microsoft", "microsoft india", "microsoft corporation"],
    "google":           ["google", "google india", "alphabet", "google llc"],
    "amazon":           ["amazon", "aws", "amazon web services", "amazon dev center",
                         "amazon india", "amazon development", "a2z"],
    "salesforce":       ["salesforce", "salesforce india", "salesforce com"],
    "oracle":           ["oracle", "oracle india", "oracle financial"],
    "sap":              ["sap", "sap india", "sap labs"],
    "cisco":            ["cisco", "cisco systems", "cisco india"],
    "ibm":              ["ibm", "ibm india", "ibm research", "kyndryl"],
    "adobe":            ["adobe", "adobe india", "adobe systems"],
    "servicenow":       ["servicenow", "service now"],
    "nvidia":           ["nvidia", "nvidia india"],
    "vmware":           ["vmware", "broadcom", "vmware india"],
    "palo alto":        ["palo alto networks", "palo alto", "paloaltonetworks"],
    "crowdstrike":      ["crowdstrike", "crowd strike", "crowdstrike india"],
    "fortinet":         ["fortinet", "fortinet india"],
    "hitachi":          ["hitachi vantara", "hitachi", "hitachi india"],
    "qualcomm":         ["qualcomm", "qualcomm india"],
    "micron":           ["micron", "micron technology", "micron india"],
    "intel":            ["intel", "intel india", "intel corporation"],
    "texas instruments":["texas instruments", "ti ", "texas instruments india"],
    "synopsys":         ["synopsys", "synopsys india"],
    "cadence":          ["cadence", "cadence design", "cadence india"],
    "nice":             ["nice systems", "nice", "nice incontact", "nice india"],
    "pegasystems":      ["pegasystems", "pega", "pega systems"],
    "opentext":         ["opentext", "open text", "micro focus"],
    "sabre":            ["sabre", "sabre corporation", "sabre india"],
    "sonatype":         ["sonatype"],
    "beyondtrust":      ["beyondtrust", "beyond trust"],
    "secureworks":      ["secureworks", "secure works", "secureworks india"],
    "sailpoint":        ["sailpoint", "sail point", "sailpoint technologies"],
    "ping identity":    ["ping identity", "pingidentity", "forgerock", "ping"],
    "dazn":             ["dazn"],
    # ── BFSI ─────────────────────────────────────────────────
    "jp morgan":        ["jpmorgan", "jp morgan", "j.p. morgan", "jpmc",
                         "jpmorgan chase", "jp morgan chase"],
    "goldman sachs":    ["goldman sachs", "goldman", "gs ", "goldman sachs india"],
    "morgan stanley":   ["morgan stanley", "morganstanley", "morgan stanley india"],
    "wells fargo":      ["wells fargo", "wellsfargo", "wells fargo india",
                         "wells fargo bank", "wells fargo & company"],
    "hsbc":             ["hsbc", "hsbc india", "hsbc bank", "hsbc holdings",
                         "hsbc software", "hsbc global"],
    "barclays":         ["barclays", "barclays india", "barclays bank",
                         "barclays shared services"],
    "deutsche bank":    ["deutsche bank", "deutschebank", "db ", "deutsche bank india"],
    "citi":             ["citi", "citibank", "citigroup", "citi india",
                         "citicorp", "citi technology"],
    "standard chartered":["standard chartered", "stanchart", "sc ", 
                          "standard chartered bank"],
    "ubs":              ["ubs", "ubs india", "ubs ag"],
    "blackrock":        ["blackrock", "black rock", "blackrock india"],
    "fidelity":         ["fidelity", "fidelity investments", "fidelity india",
                         "fmr", "fidelity management"],
    "visa":             ["visa", "visa inc", "visa india", "visa worldwide"],
    "mastercard":       ["mastercard", "master card", "mastercard india"],
    "paypal":           ["paypal", "pay pal", "paypal india"],
    "state street":     ["state street", "state street india", "ssga"],
    "swiss re":         ["swiss re", "swissre", "swiss re india"],
    "franklin templeton":["franklin templeton", "franklin", "templeton",
                          "franklin resources"],
    "vanguard":         ["vanguard", "vanguard india", "the vanguard group"],
    "metlife":          ["metlife", "met life", "metlife india", "metropolitan life"],
    "synchrony":        ["synchrony", "synchrony financial", "synchrony india"],
    "allstate":         ["allstate", "allstate india", "allstate solutions"],
    "northern trust":   ["northern trust", "northerntrust", "northern trust india"],
    "charles schwab":   ["schwab", "charles schwab", "charles schwab india"],
    "broadridge":       ["broadridge", "broad ridge", "broadridge financial",
                         "broadridge india"],
    "natwest":          ["natwest", "nat west", "natwest group", "royal bank scotland",
                         "rbs", "natwest markets"],
    "fiserv":           ["fiserv", "fis", "fidelity national", "fisglobal",
                         "fidelity national information"],
    "principal":        ["principal financial", "principal", "principal india"],
    "nationwide":       ["nationwide", "nationwide india"],
    "zurich":           ["zurich", "zurich insurance", "zurich india"],
    "manulife":         ["manulife", "john hancock", "manulife india"],
    "wex":              ["wex", "wex inc", "wright express"],
    # ── Pharma / Healthcare ───────────────────────────────────
    "novartis":         ["novartis", "novartis india", "sandoz"],
    "astrazeneca":      ["astrazeneca", "astra zeneca", "astrazeneca india"],
    "sanofi":           ["sanofi", "sanofi india", "sanofi aventis"],
    "eli lilly":        ["eli lilly", "lilly", "elanco"],
    "roche":            ["roche", "roche india", "genentech", "roche diagnostics"],
    "amgen":            ["amgen", "amgen india"],
    "thermo fisher":    ["thermo fisher", "thermofisher", "thermo fisher scientific",
                         "thermo fisher india", "life technologies"],
    "lonza":            ["lonza", "lonza india"],
    "danaher":          ["danaher", "danaher india", "beckman coulter"],
    "philips":          ["philips", "philips india", "philips healthcare"],
    "medtronic":        ["medtronic", "medtronic india"],
    "hca":              ["hca", "hca healthcare", "hca india"],
    "humana":           ["humana", "humana india"],
    "elevance":         ["elevance", "elevance health", "anthem", "wellpoint"],
    # ── Manufacturing / Engineering ───────────────────────────
    "bosch":            ["bosch", "bosch india", "robert bosch"],
    "siemens":          ["siemens", "siemens india", "siemens healthineers"],
    "abb":              ["abb", "abb india", "abb limited"],
    "honeywell":        ["honeywell", "honeywell india", "honeywell technology"],
    "ge":               ["general electric", "ge ", "ge aviation", "ge digital",
                         "ge india", "ge power"],
    "boeing":           ["boeing", "boeing india", "boeing japa"],
    "airbus":           ["airbus", "airbus india", "airbus group"],
    "shell":            ["shell", "shell india", "royal dutch shell"],
    "bp":               ["bp ", "bp india", "british petroleum", "bp plc"],
    "arcelormittal":    ["arcelormittal", "arcelor mittal", "arcelor"],
    "caterpillar":      ["caterpillar", "cat ", "caterpillar india"],
    "volvo":            ["volvo", "volvo india", "volvo group"],
    "rockwell":         ["rockwell automation", "rockwell", "rockwell india"],
    "aptiv":            ["aptiv", "aptiv india", "delphi technologies"],
    "nxp":              ["nxp", "nxp semiconductors", "nxp india"],
    "infineon":         ["infineon", "infineon technologies", "infineon india"],
    "lam research":     ["lam research", "lam", "lam research india"],
    "applied materials":["applied materials", "amat", "applied materials india"],
    "qualcomm":         ["qualcomm", "qualcomm india", "qualcomm technologies"],
    # ── Retail / Consumer ─────────────────────────────────────
    "walmart":          ["walmart", "walmart india", "walmart global tech",
                         "walmart labs", "flipkart"],
    "target":           ["target", "target india", "target corporation"],
    "flipkart":         ["flipkart", "walmart"],
    "mcdonald":         ["mcdonald", "mcdonalds", "mcdonald's"],
    "marriott":         ["marriott", "marriott india", "marriott international"],
    "tesco":            ["tesco", "tesco india", "tesco technology"],
    "unilever":         ["unilever", "unilever india", "hul", "hindustan unilever"],
    "p&g":              ["procter", "gamble", "p&g", "procter gamble",
                         "procter & gamble india"],
    # ── Consulting ────────────────────────────────────────────
    "accenture":        ["accenture", "accenture india", "accenture solutions"],
    "deloitte":         ["deloitte", "deloitte india", "deloitte consulting",
                         "deloitte touche"],
    "pwc":              ["pricewaterhousecoopers", "price waterhouse", "pwc",
                         "pwc india", "price waterhouse coopers"],
    "ey":               ["ernst", "ernst young", "ernst & young", "ey india",
                         "ernst young india"],
    "kpmg":             ["kpmg", "kpmg india"],
    "mckinsey":         ["mckinsey", "mckinsey india", "mckinsey & company"],
    "bcg":              ["boston consulting", "bcg", "bcg india"],
    "genpact":          ["genpact", "genpact india"],
    # ── Telecom ───────────────────────────────────────────────
    "at&t":             ["at&t", "att", "at t", "at&t india",
                         "at&t global", "att global"],
    "t-mobile":         ["t-mobile", "tmobile", "t mobile"],
    # ── Other ─────────────────────────────────────────────────
    "conduent":         ["conduent", "conduent india"],
    "concentrix":       ["concentrix", "concentrix india"],
    "zebra":            ["zebra technologies", "zebra", "zebra india"],
    "infosys":          ["infosys", "infosys bpm", "infosys india"],
    "ltimindtree":      ["ltimindtree", "lti mindtree", "mindtree",
                         "larsen toubro infotech"],
    "mphasis":          ["mphasis", "mphasis india"],
    "swiss re":         ["swiss re", "swissre", "swiss reinsurance"],
    "hitachi vantara":  ["hitachi vantara", "hitachi", "hitachi india"],
    "nice systems":     ["nice systems", "nice", "nice incontact"],
    "standard chartered":["standard chartered", "stanchart"],
    "texas instruments":["texas instruments", "ti ", "texas instruments india"],
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
