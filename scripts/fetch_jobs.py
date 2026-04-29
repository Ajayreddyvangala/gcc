"""
GCC Intelligence Hub — Multi-Source Nightly Job Fetcher v5
==========================================================
Sources:
  1. Playwright  — headless browser, bypasses Workday/iCIMS/SuccessFactors
                   blocks (91 Workday + Wells Fargo + HSBC + Barclays +
                   Citi + Deutsche Bank = 96 companies)
  2. Greenhouse  — direct API, 10 companies (free, no key)
  3. Lever       — direct API,  3 companies (free, no key)
  4. JSearch     — city-wide + targeted fallback (8+16 calls)
  5. Adzuna      — parallel aggregator (8 calls)
"""

import os, re, time, json, asyncio, requests
from datetime import datetime, timezone
from supabase import create_client
from playwright.async_api import async_playwright

# ── CREDENTIALS ───────────────────────────────────────────────
SUPABASE_URL = os.environ['SUPABASE_URL']
SUPABASE_KEY = os.environ['SUPABASE_KEY']
JSEARCH_KEY  = os.environ['JSEARCH_KEY']
ADZUNA_ID    = 'ef3e0dae'
ADZUNA_KEY   = '0a7e855de7f680dedbfff13bf6ad8cd9'

JSEARCH_URL  = "https://jsearch.p.rapidapi.com/search"
JSEARCH_HOST = "jsearch.p.rapidapi.com"
ADZUNA_URL   = "https://api.adzuna.com/v1/api/jobs/in/search/1"

CITIES = [
    {"key":"HYD","name":"Hyderabad","jsearch":"Hyderabad, India","adzuna":"Hyderabad"},
    {"key":"BLR","name":"Bengaluru","jsearch":"Bangalore, India", "adzuna":"Bangalore"},
]

SECURITY_TERMS = [
    "information security","cybersecurity","cyber security",
    "identity access","iam","endpoint security","cloud security",
    "security engineer","security manager","security architect",
    "security analyst","security lead","security director",
    "security officer","security head","security specialist",
    "devsecops","appsec","soc analyst","threat intel",
    "privileged access","zero trust","siem","soc manager",
    "infosec","network security","application security",
]

def is_security(title):
    t = title.lower()
    return any(s in t for s in SECURITY_TERMS)

def now_iso():
    return datetime.now(timezone.utc).isoformat()


# ═══════════════════════════════════════════════════════════════
#  SOURCE 1 — PLAYWRIGHT BROWSER  (bypasses all ATS restrictions)
# ═══════════════════════════════════════════════════════════════

# Each entry: (hyd_id, blr_id, company_name, page_url_hyd, page_url_blr)
# page_url is the actual careers search page a user would open
# Playwright loads it, intercepts the JSON the page fetches itself
PLAYWRIGHT_COMPANIES = [
    # ── Workday companies ─────────────────────────────────────
    ( 6, 104,"Goldman Sachs",
      "https://higher.gs.com/roles?query=information+security&locations=Hyderabad",
      "https://higher.gs.com/roles?query=information+security&locations=Bengaluru"),
    (11,None,"Vanguard",
      "https://www.vanguardjobs.com/search-jobs/?keyword=security&location=Hyderabad",
      None),
    (44,None,"Swiss Re",
      "https://www.swissre.com/careers/open-positions.html?field_posting_title_value=security",
      None),
    (45,None,"Franklin Templeton",
      "https://www.franklintempletoncareers.com/search-jobs/?keyword=security&location=Hyderabad",
      None),
    (58, 142,"Adobe",
      "https://careers.adobe.com/us/en/search-results?keywords=security&location=Hyderabad",
      "https://careers.adobe.com/us/en/search-results?keywords=security&location=Bengaluru"),
    (52, 119,"Bosch",
      "https://www.bosch.com/careers/find-your-job/?country=india&query=security&location=Hyderabad",
      "https://www.bosch.com/careers/find-your-job/?country=india&query=security&location=Bengaluru"),
    (42, 121,"ABB",
      "https://careers.abb/global/en/search-results?keywords=security&location=Hyderabad",
      "https://careers.abb/global/en/search-results?keywords=security&location=Bengaluru"),
    (43, 122,"Honeywell",
      "https://careers.honeywell.com/us/en/search-results?keywords=security&location=Hyderabad",
      "https://careers.honeywell.com/us/en/search-results?keywords=security&location=Bengaluru"),
    (None,129,"Medtronic",
      None,
      "https://jobs.medtronic.com/jobs/search?q=security&location=Bengaluru"),
    (None,128,"Philips",
      None,
      "https://www.careers.philips.com/global/en/search-results?keywords=security&location=Bengaluru"),
    (30, 127,"Novartis",
      "https://www.novartis.com/careers/career-search#location=Hyderabad&keyword=security",
      "https://www.novartis.com/careers/career-search#location=Bengaluru&keyword=security"),
    (31, 126,"AstraZeneca",
      "https://careers.astrazeneca.com/search-jobs?q=security&l=Hyderabad",
      "https://careers.astrazeneca.com/search-jobs?q=security&l=Bengaluru"),
    (32, 159,"Sanofi",
      "https://www.sanofi.com/en/careers/search-jobs#location=Hyderabad&keyword=security",
      "https://www.sanofi.com/en/careers/search-jobs#location=Bengaluru&keyword=security"),
    (41, 151,"Shell",
      "https://www.shell.com/careers/search-jobs.html?q=security&country=India&city=Hyderabad",
      "https://www.shell.com/careers/search-jobs.html?q=security&country=India&city=Bengaluru"),
    (55, 161,"BP",
      "https://www.bp.com/en/global/corporate/careers/searching-for-jobs.html?jobFamily=Information+Technology&location=Hyderabad",
      "https://www.bp.com/en/global/corporate/careers/searching-for-jobs.html?jobFamily=Information+Technology&location=Bengaluru"),
    (None,154,"Fiserv",
      None,
      "https://careers.fiserv.com/en/jobs/?q=security&l=Bengaluru"),
    (None,136,"BlackRock",
      None,
      "https://careers.blackrock.com/job-search-results/?q=security&location=Bengaluru"),
    (None,135,"UBS",
      None,
      "https://www.ubs.com/global/en/careers/search-jobs.html?q=security&location=Bengaluru"),
    (None,139,"Mastercard",
      None,
      "https://careers.mastercard.com/us/en/search-results?keywords=security&location=Bengaluru"),
    (None,152,"McKinsey",
      None,
      "https://www.mckinsey.com/careers/search-jobs?q=security&l=Bengaluru"),
    (None,153,"BCG",
      None,
      "https://careers.bcg.com/jobs?q=security&l=Bengaluru"),
    (None,156,"Caterpillar",
      None,
      "https://careers.caterpillar.com/en/jobs/search/?q=security&location=Bengaluru"),
    (None,157,"Volvo",
      None,
      "https://www.volvogroup.com/en/careers/search-jobs.html?q=security&l=Bengaluru"),
    (None,158,"Rockwell Automation",
      None,
      "https://jobs.rockwellautomation.com/search-jobs/security/Bengaluru"),
    (79, 176,"Thermo Fisher",
      "https://jobs.thermofisher.com/global/en/search-results?keywords=security&location=Hyderabad",
      "https://jobs.thermofisher.com/global/en/search-results?keywords=security&location=Bengaluru"),
    (85, 181,"Lam Research",
      "https://careers.lamresearch.com/search-results?keywords=security&location=Hyderabad",
      "https://careers.lamresearch.com/search-results?keywords=security&location=Bengaluru"),
    (86, 182,"Applied Materials",
      "https://careers.appliedmaterials.com/careers/search-results?keywords=security&location=Hyderabad",
      "https://careers.appliedmaterials.com/careers/search-results?keywords=security&location=Bengaluru"),
    (87, 183,"Synopsys",
      "https://careers.synopsys.com/careers/jobs?keywords=security&location=Hyderabad",
      "https://careers.synopsys.com/careers/jobs?keywords=security&location=Bengaluru"),
    (88, 184,"Cadence",
      "https://cadence.wd1.myworkdayjobs.com/en-US/External_Careers/jobs?q=security&locations=Hyderabad",
      "https://cadence.wd1.myworkdayjobs.com/en-US/External_Careers/jobs?q=security&locations=Bengaluru"),
    (89, 185,"NXP Semiconductors",
      "https://nxp.wd3.myworkdayjobs.com/careers/jobs?q=security&locations=Hyderabad",
      "https://nxp.wd3.myworkdayjobs.com/careers/jobs?q=security&locations=Bengaluru"),
    (90, 186,"Infineon",
      "https://www.infineon.com/cms/en/careers/working-at-infineon/jobsearch/?q=security&l=Hyderabad",
      "https://www.infineon.com/cms/en/careers/working-at-infineon/jobsearch/?q=security&l=Bengaluru"),
    (91, 187,"Zurich Insurance",
      "https://www.zurich.com/en/careers/search-results?q=security&l=Hyderabad",
      "https://www.zurich.com/en/careers/search-results?q=security&l=Bengaluru"),
    (92,None,"Manulife",
      "https://manulife.wd3.myworkdayjobs.com/MFCJH_Jobs/jobs?q=security",
      None),
    (97, 178,"Aptiv",
      "https://aptiv.wd5.myworkdayjobs.com/careers/jobs?q=security&locations=Hyderabad",
      "https://aptiv.wd5.myworkdayjobs.com/careers/jobs?q=security&locations=Bengaluru"),
    # ── BFSI — iCIMS / SuccessFactors / own ATS ───────────────
    ( 7, 188,"Wells Fargo",
      "https://www.wellsfargojobs.com/en/jobs/?search=information+security&country=India&city=Hyderabad",
      "https://www.wellsfargojobs.com/en/jobs/?search=information+security&country=India&city=Bengaluru"),
    ( 5, 130,"HSBC",
      "https://mycareer.hsbc.com/en_GB/external/SearchJobs/security?3_120_3=5357993&3_78_3=5357993",
      "https://mycareer.hsbc.com/en_GB/external/SearchJobs/security?3_120_3=5357993"),
    (39, 132,"Barclays",
      "https://search.jobs.barclays/search?q=security&location=Hyderabad",
      "https://search.jobs.barclays/search?q=security&location=Bengaluru"),
    (40, 134,"Citi",
      "https://jobs.citi.com/search-jobs/security/Hyderabad/287",
      "https://jobs.citi.com/search-jobs/security/Bengaluru/287"),
    (38, 131,"Deutsche Bank",
      "https://careers.db.com/search-results?keywords=security&location=Hyderabad",
      "https://careers.db.com/search-results?keywords=security&location=Bengaluru"),
    (None,105,"Morgan Stanley",
      None,
      "https://morganstanley.wd1.myworkdayjobs.com/en-US/Careers/jobs?q=security&locations=Bengaluru"),
    (None,133,"Standard Chartered",
      None,
      "https://sc.com/en/careers/job-search/?q=security&l=Bengaluru"),
    (None,137,"Fidelity",
      None,
      "https://jobs.fidelity.com/search-jobs/security/Bengaluru"),
    (None,138,"Visa",
      None,
      "https://careers.visa.com/jobs/search/?q=security&location=Bengaluru"),
    (None,140,"PayPal",
      None,
      "https://paypalcareers.com/jobs?q=security&l=Bengaluru"),
    (10,None,"MetLife",
      "https://jobs.metlife.com/search-jobs/security/Hyderabad/18/en-US",
      None),
    (11,None,"Vanguard",
      "https://www.vanguardjobs.com/search-jobs/?keyword=security&location=Hyderabad",
      None),
    (75, 189,"Synchrony",
      "https://www.synchronycareers.com/search-results?q=security&l=Hyderabad",
      "https://www.synchronycareers.com/search-results?q=security&l=Bengaluru"),
    (76,None,"Allstate",
      "https://www.allstatejobs.com/search-results?q=security&l=Hyderabad",
      None),
    (77, 175,"Northern Trust",
      "https://careers.northerntrust.com/search-results?q=security&l=Hyderabad",
      "https://careers.northerntrust.com/search-results?q=security&l=Bengaluru"),
    (78,None,"Charles Schwab",
      "https://www.schwabjobs.com/search-results?q=security&l=Hyderabad",
      None),
    (73, 173,"Broadridge",
      "https://careers.broadridge.com/search-jobs?q=security&l=Hyderabad",
      "https://careers.broadridge.com/search-jobs?q=security&l=Bengaluru"),
    (74, 174,"NatWest Group",
      "https://jobs.natwestgroup.com/search-results?q=security&l=Hyderabad",
      "https://jobs.natwestgroup.com/search-results?q=security&l=Bengaluru"),
    (93,None,"Principal Financial",
      "https://jobs.principal.com/search-results?q=security&l=Hyderabad",
      None),
    (94,None,"Nationwide",
      "https://jobs.nationwide.com/search-results?q=security&l=Hyderabad",
      None),
    (95,None,"Humana",
      "https://careers.humana.com/search-results?q=security&l=Hyderabad",
      None),
    (96, 190,"Elevance Health",
      "https://careers.elevancehealth.com/jobs/search?q=security&l=Hyderabad",
      "https://careers.elevancehealth.com/jobs/search?q=security&l=Bengaluru"),
    (84,None,"WEX Inc",
      "https://www.wexinc.com/careers/job-search?q=security&l=Hyderabad",
      None),
    # ── Pharma / Healthcare ───────────────────────────────────
    (33,None,"Eli Lilly",
      "https://careers.lilly.com/search-jobs?q=security&l=Hyderabad",
      None),
    (34,None,"HCA Healthcare",
      "https://careers.hcahealthcare.com/jobs?q=security&l=Hyderabad",
      None),
    (56,None,"Roche",
      "https://www.roche.com/careers/jobs.htm#section=joblist&location=Hyderabad&query=security",
      None),
    (72, 172,"Amgen",
      "https://careers.amgen.com/en/search-jobs?q=security&l=Hyderabad",
      "https://careers.amgen.com/en/search-jobs?q=security&l=Bengaluru"),
    (80, 177,"Danaher",
      "https://jobs.danaher.com/global/en/search-results?keywords=security&location=Hyderabad",
      "https://jobs.danaher.com/global/en/search-results?keywords=security&location=Bengaluru"),
    (67,None,"Lonza",
      "https://www.lonza.com/careers",
      None),
    # ── Manufacturing / Engineering ───────────────────────────
    (None,120,"Siemens",
      None,
      "https://jobs.siemens.com/careers?query=security&location=Bengaluru"),
    (None,123,"GE",
      None,
      "https://jobs.gecareers.com/global/en/search-results?keywords=security&location=Bengaluru"),
    (None,124,"Boeing",
      None,
      "https://jobs.boeing.com/search-jobs/security/Bengaluru"),
    (None,125,"Airbus",
      None,
      "https://www.airbus.com/en/careers/search-jobs?q=security&location=Bengaluru"),
    (64,None,"Micron Technology",
      "https://jobs.micron.com/search-jobs/security/Hyderabad/23870",
      None),
    (59,None,"Nvidia",
      "https://nvidia.wd5.myworkdayjobs.com/NVIDIAExternalCareerSite/jobs?q=security&locations=Hyderabad",
      None),
    (53,None,"Qualcomm",
      "https://careers.qualcomm.com/careers/job?q=security&location=Hyderabad",
      None),
    (None,147,"Texas Instruments",
      None,
      "https://careers.ti.com/search-results/?q=security&l=Bengaluru"),
    (None,148,"Intel",
      None,
      "https://jobs.intel.com/en/search#q=security&l=Bengaluru"),
    # ── Retail / Consumer ─────────────────────────────────────
    (36, 107,"Walmart",
      "https://careers.walmart.com/results?q=security&jobCity=Hyderabad",
      "https://careers.walmart.com/results?q=security&jobCity=Bengaluru"),
    (37, 108,"Target",
      "https://jobs.target.com/search-jobs/security/India/1479",
      "https://jobs.target.com/search-jobs/security/Bengaluru/1479"),
    (35,None,"McDonald's",
      "https://corporate.mcdonalds.com/corpmcd/Careers.html",
      None),
    (54,None,"Marriott",
      "https://jobs.marriott.com/marriott/jobs?keywords=security&location=Hyderabad",
      None),
    (None,149,"Unilever",
      None,
      "https://careers.unilever.com/search-jobs?q=security&location=Bengaluru"),
    (None,150,"P&G",
      None,
      "https://www.pgcareers.com/global/en/search-results?keywords=security&location=Bengaluru"),
    # ── IT / Tech ─────────────────────────────────────────────
    (18, 113,"Cisco",
      "https://jobs.cisco.com/jobs/SearchJobs/security?3_78_3=1073&locationLocationsCode=hyderabad",
      "https://jobs.cisco.com/jobs/SearchJobs/security?3_78_3=1073&locationLocationsCode=bangalore"),
    (17, 111,"SAP",
      "https://jobs.sap.com/search/?q=security&location=Hyderabad",
      "https://jobs.sap.com/search/?q=security&location=Bengaluru"),
    (16, 112,"Oracle",
      "https://eeho.fa.us2.oraclecloud.com/hcmUI/CandidateExperience/en/sites/jobsearch/jobs?keyword=security&locations=Hyderabad",
      "https://eeho.fa.us2.oraclecloud.com/hcmUI/CandidateExperience/en/sites/jobsearch/jobs?keyword=security&locations=Bengaluru"),
    (62, 146,"VMware",
      "https://careers.broadcom.com/jobs?q=security&location=Hyderabad",
      "https://careers.broadcom.com/jobs?q=security&location=Bengaluru"),
    (65,None,"NICE Systems",
      "https://www.nice.com/about/careers/job-openings",
      None),
    (63,None,"Hitachi Vantara",
      "https://www.hitachivantara.com/en-us/company/careers.html?q=security&location=Hyderabad",
      None),
    (71, 171,"AT&T",
      "https://www.att.jobs/search-jobs/security/India/117/1",
      "https://www.att.jobs/search-jobs/security/India/117/1"),
    (98,None,"OpenText",
      "https://careers.opentext.com/search-results?q=security&l=Hyderabad",
      None),
    (81,None,"Sabre",
      "https://careers.sabre.com/en_US/search-results?keywords=security&location=Hyderabad",
      None),
    # ── Consulting ────────────────────────────────────────────
    (19, 114,"Accenture",
      "https://www.accenture.com/in-en/careers/jobsearch?jk=security&jl=Hyderabad",
      "https://www.accenture.com/in-en/careers/jobsearch?jk=security&jl=Bengaluru"),
    (20, 115,"Deloitte",
      "https://apply.deloitte.com/careers/SearchJobs/security?3_60_3=1006905",
      "https://apply.deloitte.com/careers/SearchJobs/security?3_60_3=1006986"),
    (21, 116,"PwC",
      "https://www.pwc.in/careers/current-opportunities/search.html?q=security&location=Hyderabad",
      "https://www.pwc.in/careers/current-opportunities/search.html?q=security&location=Bengaluru"),
    (22, 117,"EY",
      "https://eyglobal.yello.co/external/requisitions?page=1&query=security+hyderabad",
      "https://eyglobal.yello.co/external/requisitions?page=1&query=security+bengaluru"),
    (23, 118,"KPMG",
      "https://kpmgcareers.kpmg.com/global/en/search-results.html?keyword=security&location=Hyderabad",
      "https://kpmgcareers.kpmg.com/global/en/search-results.html?keyword=security&location=Bengaluru"),
    (83, 180,"Genpact",
      "https://www.genpact.com/careers/job-search?q=security&l=Hyderabad",
      "https://www.genpact.com/careers/job-search?q=security&l=Bengaluru"),
]

# ── JSON patterns Playwright watches for in network responses ─
WORKDAY_API_PATTERNS = [
    # ── Workday ────────────────────────────────────────────────
    "/wday/cxs/",          # Workday REST API
    "/jobPostings",        # Workday response key
    "myworkdayjobs.com",   # Any Workday domain
    # ── iCIMS (Wells Fargo, T-Mobile, Tesco) ──────────────────
    "icims.com",           # iCIMS platform domain
    "jobs/search",         # iCIMS search endpoint
    "/jobs/search?",       # iCIMS with query params
    "searchResults",       # iCIMS response key
    # ── SuccessFactors (HSBC, Standard Chartered) ─────────────
    "/sf/careers",         # SuccessFactors endpoint
    "successfactors",      # SAP SuccessFactors
    "sap.com/careers",     # SAP careers
    "jobRequisition",      # SuccessFactors response key
    # ── Taleo (Deutsche Bank, Citi, Oracle) ───────────────────
    "taleo.net",           # Oracle Taleo
    "careersection",       # Taleo careers section
    "/careersection/",     # Taleo URL pattern
    # ── Greenhouse / Lever (already handled but catch-all) ────
    "greenhouse.io",       # Greenhouse
    "lever.co",            # Lever
    # ── Generic patterns ──────────────────────────────────────
    "api/jobs",            # Generic jobs API
    "/api/jobs",           # REST jobs endpoint
    "careers/search",      # Various ATS
    "job-search",          # Various ATS
    "/jobs?",              # Generic with query params
    "/requisitions",       # SuccessFactors requisitions
    "jobSearch",           # camelCase variant
    "JobSearch",           # PascalCase variant
    "searchjobs",          # lowercase variant
]

def looks_like_job_list(data):
    """Check if parsed JSON looks like a job list response from any ATS."""
    # Direct list of jobs
    if isinstance(data, list) and len(data) > 0:
        first = data[0]
        if isinstance(first, dict):
            return any(k in first for k in [
                "title","jobTitle","job_title","name","requisitionId",
                # iCIMS keys
                "jobtitle","jobId","listingTitle","req_title",
                # SuccessFactors keys
                "jobRequisitionId","externalJobTitle","jobTitle",
                # Taleo keys
                "jobNumber","jobPosition",
                # Generic
                "position","opening","vacancy",
            ])
    if isinstance(data, dict):
        # Standard keys
        for key in [
            # Common
            "jobPostings","jobs","results","value","items",
            "data","postings","requisitions","positions",
            # iCIMS specific
            "searchResults","jobListings","listings",
            # SuccessFactors specific
            "d","odata","jobRequisition","results",
            # Taleo specific
            "requisitionList","joblist",
            # Generic
            "vacancies","openings","careers","opportunities",
        ]:
            val = data.get(key)
            if isinstance(val, list) and len(val) > 0:
                return True
        # Check total count fields (sign of job search response)
        if any(k in data for k in
               ["totalCount","total","count","totalResults",
                "totalPositions","__count"]):
            return True
    return False

def extract_jobs_from_json(data, company, gcc_id, city_key):
    """Extract job title + URL from various JSON formats."""
    city_d = "Hyderabad" if city_key == "HYD" else "Bengaluru"
    jobs = []

    # Normalise to list — covers all ATS response formats
    items = []
    if isinstance(data, list):
        items = data
    elif isinstance(data, dict):
        for key in [
            # Workday
            "jobPostings","positions",
            # Generic
            "jobs","results","value","items","data","postings",
            # iCIMS
            "searchResults","jobListings","listings",
            # SuccessFactors
            "d","requisitions",
            # Taleo
            "requisitionList","joblist",
            # Other
            "openings","vacancies","careers","opportunities",
        ]:
            v = data.get(key)
            if isinstance(v, list) and len(v) > 0:
                items = v; break
        # SuccessFactors nested: data.d.results
        if not items:
            d = data.get("d",{})
            if isinstance(d, dict):
                v = d.get("results",[])
                if isinstance(v, list): items = v

    for item in items:
        if not isinstance(item, dict): continue
        title = (item.get("title") or item.get("jobTitle") or
                 item.get("job_title") or item.get("name") or
                 item.get("externalJobTitle") or
                 # iCIMS keys
                 item.get("jobtitle") or item.get("listingTitle") or
                 item.get("req_title") or
                 # SuccessFactors keys
                 item.get("jobRequisitionId") or
                 # Taleo keys
                 item.get("jobPosition") or item.get("jobNumber") or
                 # Generic
                 item.get("position") or item.get("opening") or "")
        if not title or not is_security(title): continue

        # URL
        url = (item.get("externalPath") or item.get("url") or
               item.get("absolute_url") or item.get("applyUrl") or
               item.get("jobUrl") or item.get("link") or
               item.get("applyUrl") or item.get("apply_url") or
               item.get("detailUrl") or item.get("jobDetailUrl") or
               item.get("redirectUrl") or "")

        # Posted date
        posted = (item.get("postedOn") or item.get("posted_at") or
                  item.get("publishedDate") or item.get("created") or
                  item.get("startDate") or "")

        job_id = (item.get("externalPath") or item.get("id") or
                  item.get("jobId") or item.get("requisitionId") or title)
        jid = re.sub(r'[^a-z0-9_]','_',
            f"pw_{company}_{city_key}_{job_id}".lower())[:120]

        jobs.append({
            "id": jid, "gcc_id": gcc_id, "company": company,
            "city": city_key, "title": title,
            "location": city_d, "url": url,
            "source": "playwright",
            "posted_date": (str(posted) if posted else None),
            "fetched_at": now_iso(),
            "is_active": True,
        })
    return jobs

async def scrape_one_page(page, url, company, gcc_id, city_key,
                           seen, timeout=25000):
    """Load one careers page and intercept job JSON responses."""
    captured = []

    async def on_response(response):
        try:
            rurl = response.url
            if response.status != 200: return
            ct = response.headers.get("content-type","")
            if "json" not in ct: return
            if not any(p in rurl for p in WORKDAY_API_PATTERNS):
                return
            body = await response.body()
            data = json.loads(body)
            if not looks_like_job_list(data): return
            jobs = extract_jobs_from_json(data, company, gcc_id, city_key)
            for j in jobs:
                if j["id"] not in seen:
                    seen.add(j["id"])
                    captured.append(j)
        except Exception:
            pass

    page.on("response", on_response)
    try:
        await page.goto(url, wait_until="networkidle", timeout=timeout)
        # Extra wait for JS-heavy pages
        await page.wait_for_timeout(3000)
    except Exception:
        pass
    page.remove_listener("response", on_response)

    # Fallback: if no JSON captured, try parsing visible job titles from DOM
    if not captured:
        try:
            # Common selectors across ATS platforms
            selectors = [
                # ── Workday ────────────────────────────────
                "li[data-automation='job-result']",
                "[class*='JobResult']",
                "[class*='job-card']",
                # ── iCIMS (Wells Fargo, T-Mobile, Tesco) ──
                ".iCIMS_JobsTable tr td",
                "[data-iCIMS-job-id]",
                ".iCIMS_ListJobTitle a",
                ".iCIMS_JobTitle",
                ".iCIMS_Job_Title",
                "[class*='iCIMS']",
                ".job-listing-item",
                ".jobs-search-results-list li",
                # ── SuccessFactors (HSBC) ──────────────────
                ".jobResultItem",
                ".TitleText",
                "[data-automation='job-result']",
                ".job-item",
                "[class*='JobItem']",
                # ── Taleo (Deutsche Bank, Citi) ────────────
                ".joblisting",
                ".requisition-title",
                "[class*='requisition']",
                # ── Wells Fargo specific ───────────────────
                ".job-search-results li",
                ".wf-job-item",
                "[data-testid*='job']",
                "article[class*='job']",
                # ── Barclays specific ──────────────────────
                "[data-testid='job-item']",
                "[class*='SearchResult']",
                # ── Generic fallbacks ──────────────────────
                "li[class*='job']",
                "div[class*='job-card']",
                "article[class*='job']",
                "[class*='job-title']",
                "[class*='jobTitle']",
                "[data-job-id]",
                ".jobs-list li",
                ".job-result-item",
                "h2[class*='job']",
                "h3[class*='job']",
                "a[class*='job-title']",
                "a[class*='jobTitle']",
                "td[class*='title'] a",
            ]
            city_d = "Hyderabad" if city_key=="HYD" else "Bengaluru"
            for sel in selectors:
                els = await page.query_selector_all(sel)
                if not els: continue
                for el in els[:30]:
                    title = (await el.get_attribute("title") or
                             await el.inner_text())
                    title = title.strip().split("\n")[0][:100]
                    if not title or not is_security(title): continue
                    link_el = await el.query_selector("a")
                    href = ""
                    if link_el:
                        href = await link_el.get_attribute("href") or ""
                    jid = re.sub(r'[^a-z0-9_]','_',
                        f"pw_{company}_{city_key}_{title}".lower())[:120]
                    if jid in seen: continue
                    seen.add(jid)
                    captured.append({
                        "id": jid, "gcc_id": gcc_id, "company": company,
                        "city": city_key, "title": title,
                        "location": city_d, "url": href,
                        "source": "playwright_dom",
                        "posted_date": "", "fetched_at": now_iso(),
                        "is_active": True,
                    })
                if captured: break
        except Exception:
            pass

    return captured

async def fetch_playwright(seen):
    """Run Playwright for all companies. Returns list of jobs."""
    all_jobs = []
    print(f"  Launching Chromium browser…")

    async with async_playwright() as pw:
        browser = await pw.chromium.launch(
            headless=True,
            args=[
                "--no-sandbox",
                "--disable-setuid-sandbox",
                "--disable-dev-shm-usage",
                "--disable-blink-features=AutomationControlled",
            ]
        )
        context = await browser.new_context(
            user_agent=("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                        "AppleWebKit/537.36 (KHTML, like Gecko) "
                        "Chrome/124.0.0.0 Safari/537.36"),
            viewport={"width": 1280, "height": 800},
            java_script_enabled=True,
            ignore_https_errors=True,
        )
        # Reuse one page for all requests
        page = await context.new_page()

        for row in PLAYWRIGHT_COMPANIES:
            hyd_id, blr_id, company = row[0], row[1], row[2]
            url_hyd, url_blr        = row[3], row[4]

            city_map = []
            if hyd_id and url_hyd:
                city_map.append((hyd_id, "HYD", url_hyd))
            if blr_id and url_blr:
                city_map.append((blr_id, "BLR", url_blr))

            for gcc_id, city_key, url in city_map:
                jobs = await scrape_one_page(
                    page, url, company, gcc_id, city_key, seen)
                if jobs:
                    all_jobs.extend(jobs)
                    print(f"  ✓ {company} {city_key}: {len(jobs)} jobs")
                else:
                    print(f"  ○ {company} {city_key}: 0 (no feed intercepted)")
                await asyncio.sleep(1.5)   # polite delay between pages

        await browser.close()
    return all_jobs


# ═══════════════════════════════════════════════════════════════
#  SOURCE 2 — GREENHOUSE DIRECT (free, works perfectly)
# ═══════════════════════════════════════════════════════════════
GH_BASE = "https://boards-api.greenhouse.io/v1/boards/{token}/jobs"

GREENHOUSE_COMPANIES = [
    (14, 144,"Palo Alto Networks","paloaltonetworks"),
    (15, 145,"CrowdStrike",       "crowdstrike"),
    (12, 141,"Salesforce",        "salesforce"),
    (57, 143,"ServiceNow",        "servicenow"),
    (48,None,"Sonatype",          "sonatype"),
    (46,None,"BeyondTrust",       "beyondtrust"),
    (47,None,"Secureworks",       "secureworks"),
    ( 9,None,"SailPoint",         "sailpoint"),
    ( 8,None,"Ping Identity",     "pingidentity"),
    (61,None,"Fortinet",          "fortinet"),
]

def fetch_greenhouse(hyd_id, blr_id, company, token, seen):
    jobs = []
    try:
        r = requests.get(GH_BASE.format(token=token),
                         params={"content":"true"}, timeout=20)
        if r.status_code != 200: return jobs
        for job in r.json().get("jobs",[]):
            title = job.get("title","")
            if not is_security(title): continue
            loc = (job.get("location",{}).get("name","") + " " +
                   " ".join(o.get("name","") for o in
                   job.get("offices",[]))).lower()
            pairs = []
            if ("hyderabad" in loc or "hyd" in loc) and hyd_id:
                pairs.append((hyd_id,"HYD","Hyderabad"))
            if any(x in loc for x in
                   ["bengaluru","bangalore","blr"]) and blr_id:
                pairs.append((blr_id,"BLR","Bengaluru"))
            if not pairs:
                if "india" in loc or not loc.strip():
                    if hyd_id: pairs.append((hyd_id,"HYD","Hyderabad"))
                    if blr_id: pairs.append((blr_id,"BLR","Bengaluru"))
            for gcc_id, ck, cd in pairs:
                jid = f"gh_{token}_{ck}_{job.get('id','')}"
                if jid in seen: continue
                seen.add(jid)
                jobs.append({
                    "id": jid,"gcc_id": gcc_id,"company": company,
                    "city": ck,"title": title,"location": cd,
                    "url": job.get("absolute_url",""),
                    "source": "greenhouse",
                    "posted_date": job.get("updated_at") or None,
                    "fetched_at": now_iso(),"is_active": True,
                })
        time.sleep(0.3)
    except Exception as e:
        print(f"    GH error {company}: {e}")
    return jobs


# ═══════════════════════════════════════════════════════════════
#  SOURCE 3 — LEVER DIRECT (free, works perfectly)
# ═══════════════════════════════════════════════════════════════
LV_BASE = "https://api.lever.co/v0/postings/{slug}?mode=json"

LEVER_COMPANIES = [
    (68,None,"DAZN",              "dazn"),
    (69,None,"Pegasystems",       "pega"),
    (None,160,"Zebra Technologies","zebra"),
]

def fetch_lever(hyd_id, blr_id, company, slug, seen):
    jobs = []
    try:
        r = requests.get(LV_BASE.format(slug=slug), timeout=20)
        if r.status_code != 200: return jobs
        data = r.json()
        if not isinstance(data, list): return jobs
        for job in data:
            title = job.get("text","")
            if not is_security(title): continue
            loc = job.get("categories",{}).get("location","").lower()
            pairs = []
            if ("hyderabad" in loc or "hyd" in loc) and hyd_id:
                pairs.append((hyd_id,"HYD","Hyderabad"))
            if any(x in loc for x in
                   ["bengaluru","bangalore","blr"]) and blr_id:
                pairs.append((blr_id,"BLR","Bengaluru"))
            if not pairs:
                if "india" in loc or not loc:
                    if hyd_id: pairs.append((hyd_id,"HYD","Hyderabad"))
                    if blr_id: pairs.append((blr_id,"BLR","Bengaluru"))
            for gcc_id, ck, cd in pairs:
                ts  = job.get("createdAt",0)
                jid = f"lv_{slug}_{ck}_{job.get('id','')}"
                if jid in seen: continue
                seen.add(jid)
                jobs.append({
                    "id": jid,"gcc_id": gcc_id,"company": company,
                    "city": ck,"title": title,"location": cd,
                    "url": job.get("hostedUrl",""),
                    "source": "lever",
                    "posted_date": (
                        datetime.fromtimestamp(ts/1000,tz=timezone.utc)
                        .isoformat() if ts else ""),
                    "fetched_at": now_iso(),"is_active": True,
                })
        time.sleep(0.3)
    except Exception as e:
        print(f"    Lever error {company}: {e}")
    return jobs


# ═══════════════════════════════════════════════════════════════
#  SOURCE 4 — JSEARCH  (city-wide + targeted)
# ═══════════════════════════════════════════════════════════════
CITY_WIDE_QUERIES = [
    "information security manager OR lead OR director OR engineer OR analyst",
    "cybersecurity manager OR lead OR director OR head OR architect OR VP",
    "identity access management IAM manager OR lead OR director OR architect",
    "PING azure security GCP endpoint security manager OR lead OR engineer",
]

TARGETED_COMPANIES_JS = [
    ("Wells Fargo",[7,188]),("HSBC",[5,130]),("Barclays",[39,132]),
    ("Goldman Sachs",[6,104]),("Morgan Stanley",[None,105]),
    ("Deutsche Bank",[38,131]),("Citi",[40,134]),("JP Morgan",[4,106]),
]

GCC_LOOKUP = {
    ("wells fargo","HYD"):7,   ("wells fargo","BLR"):188,
    ("hsbc","HYD"):5,          ("hsbc","BLR"):130,
    ("barclays","HYD"):39,     ("barclays","BLR"):132,
    ("goldman","HYD"):6,       ("goldman","BLR"):104,
    ("morgan stanley","HYD"):None,("morgan stanley","BLR"):105,
    ("deutsche bank","HYD"):38,("deutsche bank","BLR"):131,
    ("citi","HYD"):40,         ("citi","BLR"):134,
    ("jpmorgan","HYD"):4,      ("jpmorgan","BLR"):106,
    ("jp morgan","HYD"):4,     ("jp morgan","BLR"):106,
    ("microsoft","HYD"):1,     ("microsoft","BLR"):101,
    ("google","HYD"):2,        ("google","BLR"):102,
    ("amazon","HYD"):3,        ("amazon","BLR"):103,
    ("ibm","HYD"):13,          ("ibm","BLR"):110,
    ("oracle","HYD"):16,       ("oracle","BLR"):112,
    ("sap","HYD"):17,          ("sap","BLR"):111,
    ("cisco","HYD"):18,        ("cisco","BLR"):113,
    ("accenture","HYD"):19,    ("accenture","BLR"):114,
    ("deloitte","HYD"):20,     ("deloitte","BLR"):115,
    ("pwc","HYD"):21,          ("pwc","BLR"):116,
    ("pricewaterhouse","HYD"):21,("pricewaterhouse","BLR"):116,
    ("ernst","HYD"):22,        ("ernst","BLR"):117,
    ("kpmg","HYD"):23,         ("kpmg","BLR"):118,
    ("walmart","HYD"):36,      ("walmart","BLR"):107,
    ("target","HYD"):37,       ("target","BLR"):108,
    ("flipkart","HYD"):None,   ("flipkart","BLR"):109,
    ("standard chartered","HYD"):None,("standard chartered","BLR"):133,
    ("ubs","HYD"):None,        ("ubs","BLR"):135,
    ("blackrock","HYD"):None,  ("blackrock","BLR"):136,
    ("fidelity","HYD"):None,   ("fidelity","BLR"):137,
    ("visa","HYD"):None,       ("visa","BLR"):138,
    ("mastercard","HYD"):None, ("mastercard","BLR"):139,
    ("paypal","HYD"):None,     ("paypal","BLR"):140,
    ("state street","HYD"):51, ("state street","BLR"):155,
    ("synchrony","HYD"):75,    ("synchrony","BLR"):189,
    ("allstate","HYD"):76,     ("allstate","BLR"):None,
    ("northern trust","HYD"):77,("northern trust","BLR"):175,
    ("schwab","HYD"):78,       ("schwab","BLR"):None,
    ("broadridge","HYD"):73,   ("broadridge","BLR"):173,
    ("natwest","HYD"):74,      ("natwest","BLR"):174,
    ("principal","HYD"):93,    ("principal","BLR"):None,
    ("nationwide","HYD"):94,   ("nationwide","BLR"):None,
    ("humana","HYD"):95,       ("humana","BLR"):None,
    ("elevance","HYD"):96,     ("elevance","BLR"):190,
    ("anthem","HYD"):96,       ("anthem","BLR"):190,
    ("metlife","HYD"):10,      ("metlife","BLR"):None,
    ("vanguard","HYD"):11,     ("vanguard","BLR"):None,
    ("adobe","HYD"):58,        ("adobe","BLR"):142,
    ("novartis","HYD"):30,     ("novartis","BLR"):127,
    ("astrazeneca","HYD"):31,  ("astrazeneca","BLR"):126,
    ("sanofi","HYD"):32,       ("sanofi","BLR"):159,
    ("eli lilly","HYD"):33,    ("eli lilly","BLR"):None,
    ("lilly","HYD"):33,        ("lilly","BLR"):None,
    ("roche","HYD"):56,        ("roche","BLR"):None,
    ("amgen","HYD"):72,        ("amgen","BLR"):172,
    ("thermo fisher","HYD"):79,("thermo fisher","BLR"):176,
    ("danaher","HYD"):80,      ("danaher","BLR"):177,
    ("bosch","HYD"):52,        ("bosch","BLR"):119,
    ("siemens","HYD"):None,    ("siemens","BLR"):120,
    ("abb","HYD"):42,          ("abb","BLR"):121,
    ("honeywell","HYD"):43,    ("honeywell","BLR"):122,
    ("boeing","HYD"):None,     ("boeing","BLR"):124,
    ("airbus","HYD"):None,     ("airbus","BLR"):125,
    ("shell","HYD"):41,        ("shell","BLR"):151,
    ("qualcomm","HYD"):53,     ("qualcomm","BLR"):None,
    ("micron","HYD"):64,       ("micron","BLR"):None,
    ("nvidia","HYD"):59,       ("nvidia","BLR"):None,
    ("intel","HYD"):None,      ("intel","BLR"):148,
    ("texas instruments","HYD"):None,("texas instruments","BLR"):147,
    ("synopsys","HYD"):87,     ("synopsys","BLR"):183,
    ("cadence","HYD"):88,      ("cadence","BLR"):184,
    ("nxp","HYD"):89,          ("nxp","BLR"):185,
    ("infineon","HYD"):90,     ("infineon","BLR"):186,
    ("lam research","HYD"):85, ("lam research","BLR"):181,
    ("applied materials","HYD"):86,("applied materials","BLR"):182,
    ("at&t","HYD"):71,         ("at&t","BLR"):171,
    ("palo alto","HYD"):14,    ("palo alto","BLR"):144,
    ("crowdstrike","HYD"):15,  ("crowdstrike","BLR"):145,
    ("vmware","HYD"):62,       ("vmware","BLR"):146,
    ("broadcom","HYD"):62,     ("broadcom","BLR"):146,
    ("servicenow","HYD"):57,   ("servicenow","BLR"):143,
    ("sailpoint","HYD"):9,     ("sailpoint","BLR"):None,
    ("ping identity","HYD"):8, ("ping identity","BLR"):None,
    ("fortinet","HYD"):61,     ("fortinet","BLR"):None,
    ("beyondtrust","HYD"):46,  ("beyondtrust","BLR"):None,
    ("secureworks","HYD"):47,  ("secureworks","BLR"):None,
    ("genpact","HYD"):83,      ("genpact","BLR"):180,
    ("mckinsey","HYD"):None,   ("mckinsey","BLR"):152,
    ("bcg","HYD"):None,        ("bcg","BLR"):153,
    ("zurich","HYD"):91,       ("zurich","BLR"):187,
    ("manulife","HYD"):92,     ("manulife","BLR"):None,
    ("fiserv","HYD"):None,     ("fiserv","BLR"):154,
    ("caterpillar","HYD"):None,("caterpillar","BLR"):156,
    ("volvo","HYD"):None,      ("volvo","BLR"):157,
    ("rockwell","HYD"):None,   ("rockwell","BLR"):158,
    ("aptiv","HYD"):97,        ("aptiv","BLR"):178,
    ("unilever","HYD"):None,   ("unilever","BLR"):149,
    ("procter","HYD"):None,    ("procter","BLR"):150,
    ("ge ","HYD"):None,        ("ge ","BLR"):123,
    ("general electric","HYD"):None,("general electric","BLR"):123,
}

def match_company(employer, city_key):
    if not employer: return None
    en = employer.lower()
    best, best_len = None, 0
    for (frag, ck), gcc_id in GCC_LOOKUP.items():
        if ck != city_key or not gcc_id: continue
        if frag in en and len(frag) > best_len:
            best, best_len = gcc_id, len(frag)
    return best

def jsearch_call(query, headers, seen, city_key):
    jobs = []
    try:
        r = requests.get(JSEARCH_URL, headers=headers,
                         params={"query":query,"num_pages":"1",
                                 "date_posted":"month",
                                 "employment_types":"FULLTIME"},
                         timeout=30)
        r.raise_for_status()
        results = r.json().get("data",[])
        matched = 0
        city_d = "Hyderabad" if city_key=="HYD" else "Bengaluru"
        for job in results:
            jid = job.get("job_id","")
            if not jid or jid in seen: continue
            gcc_id = match_company(job.get("employer_name",""), city_key)
            if not gcc_id: continue
            seen.add(jid)
            matched += 1
            jobs.append({
                "id":jid,"gcc_id":gcc_id,
                "company":job.get("employer_name",""),
                "city":city_key,"title":job.get("job_title",""),
                "location":job.get("job_city",city_d),
                "url":job.get("job_apply_link",""),
                "source":"jsearch",
                "posted_date":job.get("job_posted_at_datetime_utc",""),
                "fetched_at":now_iso(),"is_active":True,
            })
        print(f"    → {len(results)} returned, {matched} matched")
        time.sleep(0.5)
    except Exception as e:
        print(f"    JSearch error: {e}")
    return jobs

def fetch_jsearch(seen):
    all_jobs = []
    headers  = {"X-RapidAPI-Key":JSEARCH_KEY,"X-RapidAPI-Host":JSEARCH_HOST}
    for city in CITIES:
        print(f"\n  City-wide — {city['name']}")
        for q in CITY_WIDE_QUERIES:
            print(f"    {q[:55]}…")
            all_jobs.extend(jsearch_call(
                f"{q} in {city['jsearch']}", headers, seen, city["key"]))
        print(f"\n  Targeted BFSI — {city['name']}")
        for company, _ in TARGETED_COMPANIES_JS:
            print(f"    {company}…")
            q = (f"{company} information security OR cybersecurity "
                 f"OR IAM OR identity access in {city['jsearch']}")
            all_jobs.extend(jsearch_call(q, headers, seen, city["key"]))
    return all_jobs


# ═══════════════════════════════════════════════════════════════
#  SOURCE 5 — ADZUNA (different index, parallel to JSearch)
# ═══════════════════════════════════════════════════════════════
ADZUNA_QUERIES = [
    {"what":"information security",
     "what_or":"manager lead director engineer analyst head specialist"},
    {"what":"cybersecurity",
     "what_or":"manager lead director head architect VP engineer"},
    {"what":"identity access",
     "what_or":"manager lead director architect engineer IAM analyst"},
    {"what":"PING azure security GCP endpoint IAM",
     "what_or":"manager lead director senior engineer architect"},
]

def fetch_adzuna(seen):
    all_jobs = []
    for city in CITIES:
        print(f"\n  Adzuna — {city['name']}")
        for q in ADZUNA_QUERIES:
            try:
                r = requests.get(ADZUNA_URL, params={
                    "app_id":ADZUNA_ID,"app_key":ADZUNA_KEY,
                    "what":q["what"],"what_or":q["what_or"],
                    "where":city["adzuna"],"results_per_page":"50",
                    "sort_by":"date","category":"it-jobs",
                }, timeout=20)
                if not r.ok: continue
                results = r.json().get("results",[])
                matched = 0
                city_d = "Hyderabad" if city["key"]=="HYD" else "Bengaluru"
                for job in results:
                    jid = f"az_{job.get('id','')}"
                    if jid in seen: continue
                    employer = job.get("company",{}).get("display_name","")
                    gcc_id   = match_company(employer, city["key"])
                    if not gcc_id: continue
                    seen.add(jid)
                    matched += 1
                    all_jobs.append({
                        "id":jid,"gcc_id":gcc_id,"company":employer,
                        "city":city["key"],"title":job.get("title",""),
                        "location":city_d,"url":job.get("redirect_url",""),
                        "source":"adzuna",
                        "posted_date":job.get("created",""),
                        "fetched_at":now_iso(),"is_active":True,
                    })
                print(f"    {q['what'][:40]}… → "
                      f"{len(results)} returned, {matched} matched")
                time.sleep(0.4)
            except Exception as e:
                print(f"    Adzuna error: {e}")
    return all_jobs


# ═══════════════════════════════════════════════════════════════
#  MAIN
# ═══════════════════════════════════════════════════════════════
def main():
    print(f"\n{'='*65}")
    print(f"GCC Job Fetcher v5 (Playwright + 4 sources) — "
          f"{datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}")
    print(f"Playwright: {len(PLAYWRIGHT_COMPANIES)} companies  |  "
          f"GH: {len(GREENHOUSE_COMPANIES)}  |  "
          f"Lever: {len(LEVER_COMPANIES)}  |  "
          f"JSearch+Adzuna: 8+8 calls")
    print(f"{'='*65}\n")

    sb = create_client(SUPABASE_URL, SUPABASE_KEY)
    sb.table("gcc_jobs").update({"is_active":False}).neq("id","x").execute()
    print("Marked existing jobs inactive")

    all_jobs = []; seen = set()

    # 1 — Playwright
    print(f"\n{'─'*40}")
    print(f"SOURCE 1 — Playwright ({len(PLAYWRIGHT_COMPANIES)} companies)")
    print(f"{'─'*40}")
    pw_jobs = asyncio.run(fetch_playwright(seen))
    all_jobs.extend(pw_jobs)
    print(f"  → Total: {len(pw_jobs)}")

    # 2 — Greenhouse
    print(f"\n{'─'*40}")
    print(f"SOURCE 2 — Greenhouse ({len(GREENHOUSE_COMPANIES)} companies)")
    print(f"{'─'*40}")
    gh_total = 0
    for hyd_id,blr_id,company,token in GREENHOUSE_COMPANIES:
        jobs = fetch_greenhouse(hyd_id,blr_id,company,token,seen)
        all_jobs.extend(jobs); gh_total+=len(jobs)
        if jobs: print(f"  ✓ {company}: {len(jobs)}")
    print(f"  → Total: {gh_total}")

    # 3 — Lever
    print(f"\n{'─'*40}")
    print(f"SOURCE 3 — Lever ({len(LEVER_COMPANIES)} companies)")
    print(f"{'─'*40}")
    lv_total = 0
    for hyd_id,blr_id,company,slug in LEVER_COMPANIES:
        jobs = fetch_lever(hyd_id,blr_id,company,slug,seen)
        all_jobs.extend(jobs); lv_total+=len(jobs)
        if jobs: print(f"  ✓ {company}: {len(jobs)}")
    print(f"  → Total: {lv_total}")

    # 4 — JSearch
    print(f"\n{'─'*40}")
    print("SOURCE 4 — JSearch (city-wide + targeted)")
    print(f"{'─'*40}")
    js_jobs = fetch_jsearch(seen)
    all_jobs.extend(js_jobs)
    print(f"  → Total: {len(js_jobs)}")

    # 5 — Adzuna
    print(f"\n{'─'*40}")
    print("SOURCE 5 — Adzuna")
    print(f"{'─'*40}")
    az_jobs = fetch_adzuna(seen)
    all_jobs.extend(az_jobs)
    print(f"  → Total: {len(az_jobs)}")

    # Save to Supabase
    print(f"\n{'─'*40}")
    print(f"💾 Saving {len(all_jobs)} jobs to Supabase…")
    if all_jobs:
        def clean(job):
            """Ensure no empty strings in typed columns."""
            j = dict(job)
            j["posted_date"] = j.get("posted_date") or None
            j["url"]         = j.get("url") or ""
            j["location"]    = j.get("location") or ""
            return j
        cleaned = [clean(j) for j in all_jobs]
        for i in range(0, len(cleaned), 100):
            sb.table("gcc_jobs").upsert(
                cleaned[i:i+100], on_conflict="id").execute()

    companies = len(set(j["gcc_id"] for j in all_jobs))
    try:
        sb.table("gcc_fetch_status").insert({
        "fetched_at":          now_iso(),
        "total_jobs":          len(all_jobs),
        "companies_with_jobs": companies,
        }).execute()
    except Exception as e:
        print(f"  Status save warning: {e}")

    print(f"\n{'='*65}")
    print(f"✅ COMPLETE")
    print(f"   Playwright:  {len(pw_jobs)}")
    print(f"   Greenhouse:  {gh_total}")
    print(f"   Lever:       {lv_total}")
    print(f"   JSearch:     {len(js_jobs)}")
    print(f"   Adzuna:      {len(az_jobs)}")
    print(f"   TOTAL:       {len(all_jobs)} jobs, {companies} companies")
    print(f"{'='*65}\n")

if __name__ == "__main__":
    main()
