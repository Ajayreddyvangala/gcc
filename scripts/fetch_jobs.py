"""
GCC Intelligence Hub — Multi-Source Nightly Job Fetcher v3
Sources:
  1. Workday    — direct ATS API (91 companies, free, no key)
  2. Greenhouse — direct ATS API (10 companies, free, no key)
  3. Lever      — direct ATS API ( 3 companies, free, no key)
  4. JSearch    — aggregator fallback (8 calls, RapidAPI free)
"""

import os, re, time, requests
from datetime import datetime, timezone
from supabase import create_client

# ── CREDENTIALS ───────────────────────────────────────────────
SUPABASE_URL = os.environ['SUPABASE_URL']
SUPABASE_KEY = os.environ['SUPABASE_KEY']
JSEARCH_KEY  = os.environ['JSEARCH_KEY']

JSEARCH_URL  = "https://jsearch.p.rapidapi.com/search"
JSEARCH_HOST = "jsearch.p.rapidapi.com"

JSEARCH_QUERIES = [
    "information security manager OR lead OR director OR engineer OR analyst OR architect",
    "cybersecurity manager OR lead OR director OR head OR architect OR VP OR engineer",
    "identity access management IAM manager OR lead OR director OR engineer OR architect",
    "PING azure active directory security GCP endpoint security manager OR lead OR engineer",
]

CITIES = [
    {"key":"HYD","name":"Hyderabad","jsearch":"Hyderabad, India","wd":"Hyderabad"},
    {"key":"BLR","name":"Bengaluru","jsearch":"Bangalore, India", "wd":"Bangalore"},
]

# Security role keywords for filtering direct ATS results
SECURITY_TERMS = [
    "information security","cybersecurity","cyber security",
    "identity access","iam","endpoint security","cloud security",
    "security engineer","security manager","security architect",
    "security analyst","security lead","security director",
    "security officer","security head","security specialist",
    "devsecops","appsec","soc analyst","threat intel",
    "privileged access","zero trust","siem","soc manager",
]

# ── WORKDAY COMPANIES (91 total) ──────────────────────────────
# Format: (hyd_id, blr_id, company_name, workday_base_url)
WORKDAY_COMPANIES = [
    # ── Original 34 ──────────────────────────────────────────
    ( 6, 104,"Goldman Sachs",       "higher.gs.com/wday/cxs/gs/External"),
    (11,None,"Vanguard",            "vanguard.wd5.myworkdayjobs.com/wday/cxs/vanguard/Vanguard_Careers_Worldwide"),
    (44,None,"Swiss Re",            "swissre.wd3.myworkdayjobs.com/wday/cxs/Swissre/Swissre"),
    (45,None,"Franklin Templeton",  "franklintempleton.wd5.myworkdayjobs.com/wday/cxs/FT/FT_External_Career_Site"),
    (58, 142,"Adobe",               "adobe.wd5.myworkdayjobs.com/wday/cxs/external/external"),
    (52, 119,"Bosch",               "bosch.wd3.myworkdayjobs.com/wday/cxs/bosch/Bosch_Ext_Careers"),
    (42, 121,"ABB",                 "careers.abb/wday/cxs/abb/ABBglobal"),
    (43, 122,"Honeywell",           "honeywell.wd5.myworkdayjobs.com/wday/cxs/Honeywell/HoneywellCareers"),
    (None,129,"Medtronic",          "medtronic.wd5.myworkdayjobs.com/wday/cxs/MedtronicCareers/Global"),
    (None,128,"Philips",            "philips.wd3.myworkdayjobs.com/wday/cxs/PhilipsExternalCareers/External_Philips_Careers"),
    (30, 127,"Novartis",            "novartis.wd3.myworkdayjobs.com/wday/cxs/Novartis/Novartis_External_Careers"),
    (31, 126,"AstraZeneca",         "astrazeneca.wd3.myworkdayjobs.com/wday/cxs/AZExternalSite/AstraZeneca"),
    (32, 159,"Sanofi",              "sanofi.wd3.myworkdayjobs.com/wday/cxs/Sanofi/Sanofi_Careers"),
    (41, 151,"Shell",               "shell.wd3.myworkdayjobs.com/wday/cxs/ShellExt/ShellExternalCareer"),
    (55, 161,"BP",                  "bp.wd3.myworkdayjobs.com/wday/cxs/bpjobs/BP_External_Careers"),
    (None,154,"Fiserv",             "fiserv.wd5.myworkdayjobs.com/wday/cxs/Fiserv_Careers/FiservCareers"),
    (None,136,"BlackRock",          "blackrock.wd1.myworkdayjobs.com/wday/cxs/BlackRock/BlackRock_Careers"),
    (None,135,"UBS",                "ubs.wd3.myworkdayjobs.com/wday/cxs/UBS/UBS_Global"),
    (None,139,"Mastercard",         "mastercard.wd1.myworkdayjobs.com/wday/cxs/CorporateCareers/Mastercard_Careers"),
    (None,152,"McKinsey",           "mckinsey.wd1.myworkdayjobs.com/wday/cxs/mckinseyandcompany/McKinsey"),
    (None,153,"BCG",                "bcg.wd3.myworkdayjobs.com/wday/cxs/BCGCareers/BCG_Careers"),
    (None,156,"Caterpillar",        "cat.wd5.myworkdayjobs.com/wday/cxs/CaterpillarCareers/CaterpillarJobPostings"),
    (None,157,"Volvo",              "volvo.wd3.myworkdayjobs.com/wday/cxs/Volvo_Group_External/VolvoGroupCareers"),
    (None,158,"Rockwell Automation","rockwellautomation.wd5.myworkdayjobs.com/wday/cxs/Rockwell_Automation/Global"),
    (79, 176,"Thermo Fisher",       "thermofisher.wd5.myworkdayjobs.com/wday/cxs/TFSCareers/ThermoFisherScientificCareers"),
    (85, 181,"Lam Research",        "lamresearch.wd1.myworkdayjobs.com/wday/cxs/LamCareers/LamResearchCareers"),
    (86, 182,"Applied Materials",   "amat.wd1.myworkdayjobs.com/wday/cxs/External/AppliedMaterials"),
    (87, 183,"Synopsys",            "synopsys.wd5.myworkdayjobs.com/wday/cxs/SynopsysCareers/SynopsysCareerSite"),
    (88, 184,"Cadence",             "cadence.wd1.myworkdayjobs.com/wday/cxs/External/CadenceExternalCareers"),
    (89, 185,"NXP Semiconductors",  "nxp.wd3.myworkdayjobs.com/wday/cxs/nxpcareers/NXPCareers"),
    (90, 186,"Infineon",            "infineon.wd3.myworkdayjobs.com/wday/cxs/External/Infineon_Careers"),
    (91, 187,"Zurich Insurance",    "zurich.wd3.myworkdayjobs.com/wday/cxs/Zurich/Zurich_Careers"),
    (92,None,"Manulife",            "manulife.wd3.myworkdayjobs.com/wday/cxs/MFCJH_Jobs/ManulifeCareers"),
    (97, 178,"Aptiv",               "aptiv.wd5.myworkdayjobs.com/wday/cxs/careers/AptivCareers"),
    # ── New: BFSI ─────────────────────────────────────────────
    ( 7, 188,"Wells Fargo",         "wellsfargo.wd5.myworkdayjobs.com/wday/cxs/WellsFargo/WellsFargoJobsGlobal"),
    ( 5, 130,"HSBC",                "mycareer.hsbc.com/wday/cxs/hsbc/External_Careers"),
    (39, 132,"Barclays",            "barclays.wd3.myworkdayjobs.com/wday/cxs/Barclays/Global"),
    (40, 134,"Citi",                "citi.wd5.myworkdayjobs.com/wday/cxs/External/Citi_Global"),
    (51, 155,"State Street",        "statestreet.wd1.myworkdayjobs.com/wday/cxs/Global/State_Street"),
    (None,105,"Morgan Stanley",     "morganstanley.wd1.myworkdayjobs.com/wday/cxs/Careers/MS_Careers"),
    (None,133,"Standard Chartered", "standardchartered.wd3.myworkdayjobs.com/wday/cxs/SCJOBS/Standard_Chartered_External"),
    (None,137,"Fidelity",           "fidelityinvestments.wd5.myworkdayjobs.com/wday/cxs/Fidelity_Investments/External"),
    (None,138,"Visa",               "visa.wd5.myworkdayjobs.com/wday/cxs/visa/External"),
    (None,140,"PayPal",             "paypal.wd1.myworkdayjobs.com/wday/cxs/jobs/PayPal_Careers"),
    (10, None,"MetLife",            "metlife.wd5.myworkdayjobs.com/wday/cxs/MetLife/Global"),
    (75, 189,"Synchrony",           "synchrony.wd5.myworkdayjobs.com/wday/cxs/ExternalSite/Synchrony_External"),
    (76, None,"Allstate",           "allstate.wd5.myworkdayjobs.com/wday/cxs/allstate/AllState"),
    (77, 175,"Northern Trust",      "northerntrust.wd5.myworkdayjobs.com/wday/cxs/Careers/NorthernTrustCareers"),
    (78, None,"Charles Schwab",     "schwab.wd5.myworkdayjobs.com/wday/cxs/Employment/CharlesSchwab"),
    (73, 173,"Broadridge",          "broadridge.wd5.myworkdayjobs.com/wday/cxs/BroadridgeJobsGlobal/External"),
    (74, 174,"NatWest Group",       "natwestgroup.wd3.myworkdayjobs.com/wday/cxs/NatWestGroupCareers/NatWest_Group_Careers"),
    (93, None,"Principal Financial","principal.wd5.myworkdayjobs.com/wday/cxs/External/Principal"),
    (94, None,"Nationwide",         "nationwide.wd5.myworkdayjobs.com/wday/cxs/External/Nationwide_Careers"),
    (95, None,"Humana",             "humana.wd5.myworkdayjobs.com/wday/cxs/Humana/External"),
    (96, 190,"Elevance Health",     "elevancehealth.wd5.myworkdayjobs.com/wday/cxs/External/ElevanceHealthCareers"),
    (84, None,"WEX Inc",            "wexinc.wd5.myworkdayjobs.com/wday/cxs/WEXCareers/WEX_External_Careers"),
    # ── New: Pharma / Healthcare ──────────────────────────────
    (33, None,"Eli Lilly",          "lilly.wd5.myworkdayjobs.com/wday/cxs/LillyUS/LillyCareers"),
    (34, None,"HCA Healthcare",     "hca.wd5.myworkdayjobs.com/wday/cxs/HCA/HCA_External_Careers"),
    (56, None,"Roche",              "roche.wd3.myworkdayjobs.com/wday/cxs/roche_ext/External"),
    (72, 172,"Amgen",               "amgen.wd5.myworkdayjobs.com/wday/cxs/AmgenCareers/External"),
    (80, 177,"Danaher",             "danaher.wd5.myworkdayjobs.com/wday/cxs/DanaherCareers/DHR_Global"),
    (67, None,"Lonza",              "lonza.wd3.myworkdayjobs.com/wday/cxs/lonza/LonzaCareers"),
    # ── New: Manufacturing / Engineering ─────────────────────
    (None,120,"Siemens",            "jobs.siemens.com/wday/cxs/siemens/External"),
    (None,123,"GE",                 "jobs.gecareers.com/wday/cxs/gecareers/Global"),
    (None,124,"Boeing",             "boeing.wd1.myworkdayjobs.com/wday/cxs/External/EXTERNAL_CAREER_SITE"),
    (None,125,"Airbus",             "airbus.wd3.myworkdayjobs.com/wday/cxs/airbus/Airbus_Careers"),
    (64, None,"Micron Technology",  "micron.wd1.myworkdayjobs.com/wday/cxs/External/Micron"),
    (59, None,"Nvidia",             "nvidia.wd5.myworkdayjobs.com/wday/cxs/NVIDIAExternalCareerSite/External"),
    (53, None,"Qualcomm",           "qualcomm.wd5.myworkdayjobs.com/wday/cxs/External/Qualcomm_External"),
    (None,147,"Texas Instruments",  "careers.ti.com/wday/cxs/TI_External_Careers/External"),
    (None,148,"Intel",              "intel.wd1.myworkdayjobs.com/wday/cxs/External/Global"),
    # ── New: Retail / Consumer ────────────────────────────────
    (36, 107,"Walmart",             "walmart.wd5.myworkdayjobs.com/wday/cxs/WalmartExternal/WalmartCareers"),
    (37, 108,"Target",              "target.wd5.myworkdayjobs.com/wday/cxs/TargetExternal/IHUB"),
    (35, None,"McDonald's",         "mcdonalds.wd5.myworkdayjobs.com/wday/cxs/External/McDonalds_External"),
    (54, None,"Marriott",           "marriott.wd5.myworkdayjobs.com/wday/cxs/marriott/Global"),
    (None,149,"Unilever",           "unilever.wd3.myworkdayjobs.com/wday/cxs/External/Global"),
    (None,150,"P&G",                "pgcareers.wd5.myworkdayjobs.com/wday/cxs/Global/ProcterAndGamble"),
    # ── New: IT / Tech ────────────────────────────────────────
    (18, 113,"Cisco",               "cisco.wd5.myworkdayjobs.com/wday/cxs/Cisco/Global"),
    (17, 111,"SAP",                 "sap.wd3.myworkdayjobs.com/wday/cxs/External/SAP_Careers"),
    (16, 112,"Oracle",              "oracle.wd1.myworkdayjobs.com/wday/cxs/External/External"),
    (62, 146,"VMware",              "vmware.wd1.myworkdayjobs.com/wday/cxs/External/VMware"),
    (65, None,"NICE Systems",       "nice.wd5.myworkdayjobs.com/wday/cxs/NICEExternalCareers/External"),
    (63, None,"Hitachi Vantara",    "hitachivantara.wd1.myworkdayjobs.com/wday/cxs/hitachivantara/External"),
    (71, 171,"AT&T",                "att.wd5.myworkdayjobs.com/wday/cxs/External/att_global"),
    (98, None,"OpenText",           "opentext.wd3.myworkdayjobs.com/wday/cxs/careers/OpenText_External"),
    (81, None,"Sabre",              "sabre.wd3.myworkdayjobs.com/wday/cxs/SabreCareers/External"),
    # ── New: Consulting ───────────────────────────────────────
    (19, 114,"Accenture",           "accenture.wd103.myworkdayjobs.com/wday/cxs/AccentureCareersSite/External"),
    (20, 115,"Deloitte",            "deloitte.wd5.myworkdayjobs.com/wday/cxs/DTUSACareers/External"),
    (21, 116,"PwC",                 "pwc.wd3.myworkdayjobs.com/wday/cxs/Global/PricewaterhouseCoopers"),
    (22, 117,"EY",                  "ey.wd5.myworkdayjobs.com/wday/cxs/EYJobsBroader/EYCareers"),
    (23, 118,"KPMG",                "kpmg.wd5.myworkdayjobs.com/wday/cxs/KPMGExternal/KPMG_External"),
    (83, 180,"Genpact",             "genpact.wd5.myworkdayjobs.com/wday/cxs/External/GenpactCareers"),
]

# ── GREENHOUSE COMPANIES (10) ─────────────────────────────────
GREENHOUSE_COMPANIES = [
    (14, 144,"Palo Alto Networks",  "paloaltonetworks"),
    (15, 145,"CrowdStrike",         "crowdstrike"),
    (12, 141,"Salesforce",          "salesforce"),
    (57, 143,"ServiceNow",          "servicenow"),
    (48, None,"Sonatype",           "sonatype"),
    (46, None,"BeyondTrust",        "beyondtrust"),
    (47, None,"Secureworks",        "secureworks"),
    ( 9, None,"SailPoint",          "sailpoint"),
    ( 8, None,"Ping Identity",      "pingidentity"),
    (61, None,"Fortinet",           "fortinet"),
]

# ── LEVER COMPANIES (3) ───────────────────────────────────────
LEVER_COMPANIES = [
    (68, None,"DAZN",               "dazn"),
    (69, None,"Pegasystems",        "pega"),
    (None,160,"Zebra Technologies", "zebra"),
]

# ── JSEARCH FALLBACK — companies NOT on Workday/GH/Lever ──────
JSEARCH_GCC_LIST = [
    # HYD
    (1,  "Microsoft",          "HYD"),(2,  "Google",             "HYD"),
    (3,  "Amazon",             "HYD"),(4,  "JP Morgan Chase",    "HYD"),
    (13, "IBM",                "HYD"),(33, "Eli Lilly",          "HYD"),
    (38, "Deutsche Bank",      "HYD"),(49, "T-Mobile",           "HYD"),
    (50, "Tesco",              "HYD"),(51, "State Street",       "HYD"),
    (70, "ArcelorMittal",      "HYD"),(72, "Amgen",              "HYD"),
    (74, "NatWest Group",      "HYD"),(75, "Synchrony Financial","HYD"),
    (77, "Northern Trust",     "HYD"),(78, "Charles Schwab",     "HYD"),
    (82, "FIS",                "HYD"),(93, "Principal Financial","HYD"),
    (94, "Nationwide",         "HYD"),(95, "Humana",             "HYD"),
    (99, "Conduent",           "HYD"),
    # BLR
    (101,"Microsoft",          "BLR"),(102,"Google",             "BLR"),
    (103,"Amazon",             "BLR"),(106,"JP Morgan Chase",    "BLR"),
    (109,"Flipkart",           "BLR"),(110,"IBM",                "BLR"),
    (131,"Deutsche Bank",      "BLR"),(163,"State Street",       "BLR"),
    (171,"AT&T",               "BLR"),(172,"Amgen",              "BLR"),
    (173,"Broadridge",         "BLR"),(174,"NatWest Group",      "BLR"),
    (175,"Northern Trust",     "BLR"),(177,"Danaher",            "BLR"),
    (179,"FIS",                "BLR"),(189,"Synchrony Financial","BLR"),
    (190,"Elevance Health",    "BLR"),
]

# ── COMPANY ALIASES ───────────────────────────────────────────
ALIASES = {
    "microsoft":         ["microsoft","microsoft india","microsoft corporation"],
    "google":            ["google","google india","alphabet","google llc"],
    "amazon":            ["amazon","aws","amazon web services","amazon dev center","amazon india"],
    "salesforce":        ["salesforce","salesforce india","salesforce com"],
    "oracle":            ["oracle","oracle india","oracle financial"],
    "sap":               ["sap","sap india","sap labs"],
    "cisco":             ["cisco","cisco systems","cisco india"],
    "ibm":               ["ibm","ibm india","ibm research"],
    "adobe":             ["adobe","adobe india","adobe systems"],
    "servicenow":        ["servicenow","service now"],
    "nvidia":            ["nvidia","nvidia india"],
    "vmware":            ["vmware","broadcom","vmware india"],
    "palo alto":         ["palo alto networks","palo alto","paloaltonetworks"],
    "crowdstrike":       ["crowdstrike","crowd strike"],
    "fortinet":          ["fortinet","fortinet india"],
    "hitachi vantara":   ["hitachi vantara","hitachi","hitachi india"],
    "qualcomm":          ["qualcomm","qualcomm india","qualcomm technologies"],
    "micron":            ["micron","micron technology","micron india"],
    "intel":             ["intel","intel india","intel corporation"],
    "texas instruments": ["texas instruments","ti india","texas instruments india"],
    "synopsys":          ["synopsys","synopsys india"],
    "cadence":           ["cadence","cadence design","cadence india"],
    "nice":              ["nice systems","nice","nice incontact","nice india"],
    "pegasystems":       ["pegasystems","pega","pega systems"],
    "opentext":          ["opentext","open text","micro focus"],
    "sabre":             ["sabre","sabre corporation","sabre india"],
    "sonatype":          ["sonatype"],
    "beyondtrust":       ["beyondtrust","beyond trust"],
    "secureworks":       ["secureworks","secure works"],
    "sailpoint":         ["sailpoint","sail point","sailpoint technologies"],
    "ping identity":     ["ping identity","pingidentity","forgerock","ping"],
    "dazn":              ["dazn"],
    "jp morgan":         ["jpmorgan","jp morgan","j.p. morgan","jpmc","jpmorgan chase"],
    "goldman sachs":     ["goldman sachs","goldman","goldman sachs india"],
    "morgan stanley":    ["morgan stanley","morganstanley","morgan stanley india"],
    "wells fargo":       ["wells fargo","wellsfargo","wells fargo india",
                          "wells fargo bank","wells fargo & company"],
    "hsbc":              ["hsbc","hsbc india","hsbc bank","hsbc global",
                          "hsbc software","hsbc holdings"],
    "barclays":          ["barclays","barclays india","barclays bank",
                          "barclays shared services"],
    "deutsche bank":     ["deutsche bank","deutschebank","db india","deutsche bank india"],
    "citi":              ["citi","citibank","citigroup","citi india","citicorp"],
    "standard chartered":["standard chartered","stanchart","standard chartered bank"],
    "ubs":               ["ubs","ubs india","ubs ag"],
    "blackrock":         ["blackrock","black rock","blackrock india"],
    "fidelity":          ["fidelity","fidelity investments","fidelity india","fmr"],
    "visa":              ["visa","visa inc","visa india","visa worldwide"],
    "mastercard":        ["mastercard","master card","mastercard india"],
    "paypal":            ["paypal","pay pal","paypal india"],
    "state street":      ["state street","state street india","ssga"],
    "swiss re":          ["swiss re","swissre","swiss reinsurance"],
    "franklin templeton":["franklin templeton","franklin","templeton","franklin resources"],
    "vanguard":          ["vanguard","vanguard india","the vanguard group"],
    "metlife":           ["metlife","met life","metlife india","metropolitan life"],
    "synchrony":         ["synchrony","synchrony financial","synchrony india"],
    "allstate":          ["allstate","allstate india","allstate solutions"],
    "northern trust":    ["northern trust","northerntrust","northern trust india"],
    "charles schwab":    ["schwab","charles schwab","charles schwab india"],
    "broadridge":        ["broadridge","broad ridge","broadridge financial","broadridge india"],
    "natwest":           ["natwest","nat west","natwest group","rbs",
                          "royal bank scotland","natwest markets"],
    "fiserv":            ["fiserv","fis","fidelity national","fisglobal",
                          "fidelity national information"],
    "principal":         ["principal financial","principal","principal india"],
    "nationwide":        ["nationwide","nationwide india"],
    "zurich":            ["zurich","zurich insurance","zurich india"],
    "manulife":          ["manulife","john hancock","manulife india"],
    "wex":               ["wex","wex inc","wright express"],
    "novartis":          ["novartis","novartis india","sandoz"],
    "astrazeneca":       ["astrazeneca","astra zeneca","astrazeneca india"],
    "sanofi":            ["sanofi","sanofi india","sanofi aventis"],
    "eli lilly":         ["eli lilly","lilly","lilly india"],
    "roche":             ["roche","roche india","genentech","roche diagnostics"],
    "amgen":             ["amgen","amgen india"],
    "thermo fisher":     ["thermo fisher","thermofisher","thermo fisher scientific"],
    "lonza":             ["lonza","lonza india"],
    "danaher":           ["danaher","danaher india","beckman coulter"],
    "philips":           ["philips","philips india","philips healthcare"],
    "medtronic":         ["medtronic","medtronic india"],
    "hca":               ["hca","hca healthcare","hca india"],
    "humana":            ["humana","humana india"],
    "elevance":          ["elevance","elevance health","anthem","wellpoint"],
    "bosch":             ["bosch","bosch india","robert bosch"],
    "siemens":           ["siemens","siemens india","siemens healthineers"],
    "abb":               ["abb","abb india","abb limited"],
    "honeywell":         ["honeywell","honeywell india","honeywell technology"],
    "ge":                ["general electric","ge aviation","ge digital","ge india","ge power"],
    "boeing":            ["boeing","boeing india"],
    "airbus":            ["airbus","airbus india","airbus group"],
    "shell":             ["shell","shell india","royal dutch shell"],
    "bp":                ["bp india","british petroleum","bp plc","bp exploration"],
    "arcelormittal":     ["arcelormittal","arcelor mittal","arcelor"],
    "caterpillar":       ["caterpillar","caterpillar india"],
    "volvo":             ["volvo","volvo india","volvo group"],
    "rockwell":          ["rockwell automation","rockwell","rockwell india"],
    "aptiv":             ["aptiv","aptiv india","delphi technologies"],
    "nxp":               ["nxp","nxp semiconductors","nxp india"],
    "infineon":          ["infineon","infineon technologies","infineon india"],
    "lam research":      ["lam research","lam research india"],
    "applied materials": ["applied materials","amat","applied materials india"],
    "walmart":           ["walmart","walmart india","walmart global tech","walmart labs"],
    "target":            ["target","target india","target corporation"],
    "flipkart":          ["flipkart"],
    "mcdonald":          ["mcdonald","mcdonalds","mcdonald's"],
    "marriott":          ["marriott","marriott india","marriott international"],
    "tesco":             ["tesco","tesco india","tesco technology"],
    "unilever":          ["unilever","unilever india","hul","hindustan unilever"],
    "p&g":               ["procter","gamble","p&g","procter gamble",
                          "procter & gamble india"],
    "accenture":         ["accenture","accenture india","accenture solutions"],
    "deloitte":          ["deloitte","deloitte india","deloitte consulting","deloitte touche"],
    "pwc":               ["pricewaterhousecoopers","price waterhouse","pwc","pwc india"],
    "ey":                ["ernst","ernst young","ernst & young","ey india"],
    "kpmg":              ["kpmg","kpmg india"],
    "mckinsey":          ["mckinsey","mckinsey india","mckinsey & company"],
    "bcg":               ["boston consulting","bcg","bcg india"],
    "genpact":           ["genpact","genpact india"],
    "at&t":              ["at&t","att","at t","at&t india","att global"],
    "t-mobile":          ["t-mobile","tmobile","t mobile"],
    "conduent":          ["conduent","conduent india"],
    "zebra":             ["zebra technologies","zebra","zebra india"],
    "nice systems":      ["nice systems","nice","nice incontact"],
}


def norm(s):
    s = s.lower()
    s = re.sub(r'[^a-z0-9 ]',' ',s)
    s = re.sub(r'\b(inc|llc|ltd|limited|corp|corporation|group|india|pvt|'
               r'private|company|the|and|solutions|services|technology|'
               r'technologies|global|tech|bank|financial|consulting|'
               r'software|systems|international)\b',' ',s)
    return re.sub(r'\s+',' ',s).strip()

def get_variants(name):
    v = {norm(name)}
    nl = name.lower()
    for key,alts in ALIASES.items():
        if key in nl or any(a in nl for a in alts):
            v.update(alts)
    words = norm(name).split()
    if words: v.add(words[0])
    return v

def match_gcc_jsearch(employer, city_key):
    if not employer: return None
    en = norm(employer)
    for gcc_id,gcc_name,gcc_city in JSEARCH_GCC_LIST:
        if gcc_city != city_key: continue
        for v in get_variants(gcc_name):
            nv = norm(v)
            if nv and (en==nv or en.startswith(nv) or nv in en):
                return gcc_id
    return None

def is_security_role(title):
    t = title.lower()
    return any(term in t for term in SECURITY_TERMS)

def make_job(gcc_id, company, city_key, city_display,
             title, url, posted, source, job_id):
    return {
        "id":          job_id[:120],
        "gcc_id":      gcc_id,
        "company":     company,
        "city":        city_key,
        "title":       title,
        "location":    city_display,
        "url":         url,
        "source":      source,
        "posted_date": posted,
        "fetched_at":  datetime.now(timezone.utc).isoformat(),
        "is_active":   True,
    }


# ═══════════════════════════════════════════════════════════════
#  SOURCE 1 — WORKDAY DIRECT
# ═══════════════════════════════════════════════════════════════
WD_SEARCH_TERMS = [
    "information security",
    "cybersecurity",
    "identity access management",
    "cloud security",
]

def fetch_workday(hyd_id, blr_id, company, base_url, seen):
    jobs = []
    city_map = []
    if hyd_id: city_map.append((hyd_id,"HYD","Hyderabad"))
    if blr_id: city_map.append((blr_id,"BLR","Bangalore"))

    for gcc_id, city_key, city_wd in city_map:
        for term in WD_SEARCH_TERMS:
            try:
                url = f"https://{base_url}/jobs"
                # Try POST first (standard Workday API)
                payload = {"searchText": term,
                           "appliedFacets": {"locationCountry": ["IND"]},
                           "limit": 20, "offset": 0}
                r = requests.post(url, json=payload,
                                  headers={"Content-Type":"application/json"},
                                  timeout=15)
                if r.status_code not in (200,201):
                    # Try GET fallback
                    r = requests.get(url,
                        params={"q":term,"locations":city_wd},
                        timeout=15)
                if r.status_code not in (200,201):
                    continue
                data = r.json()
                results = (data.get("jobPostings") or
                           data.get("jobs") or [])
                for job in results:
                    title = (job.get("title") or
                             job.get("jobTitle") or "")
                    if not is_security_role(title):
                        continue
                    loc = str(job.get("locationsText") or
                              job.get("primaryLocation") or
                              job.get("location") or "").lower()
                    if loc and city_wd.lower() not in loc and \
                       "india" not in loc:
                        continue
                    ext = (job.get("externalPath") or
                           job.get("id") or title)
                    jid = re.sub(r'[^a-z0-9_]','_',
                        f"wd_{company}_{city_key}_{ext}".lower())
                    if jid in seen: continue
                    seen.add(jid)
                    apply = job.get("externalPath","")
                    if apply and not apply.startswith("http"):
                        host = base_url.split("/wday")[0]
                        apply = f"https://{host}{apply}"
                    jobs.append(make_job(
                        gcc_id, company, city_key,
                        "Hyderabad" if city_key=="HYD" else "Bengaluru",
                        title, apply or f"https://{base_url}/jobs",
                        job.get("postedOn",""), "workday", jid))
                time.sleep(0.25)
            except Exception:
                pass
    return jobs


# ═══════════════════════════════════════════════════════════════
#  SOURCE 2 — GREENHOUSE DIRECT
# ═══════════════════════════════════════════════════════════════
GH_BASE = "https://boards-api.greenhouse.io/v1/boards/{token}/jobs"

def fetch_greenhouse(hyd_id, blr_id, company, token, seen):
    jobs = []
    try:
        r = requests.get(GH_BASE.format(token=token),
                         params={"content":"true"}, timeout=20)
        if r.status_code != 200: return jobs
        for job in r.json().get("jobs",[]):
            title = job.get("title","")
            if not is_security_role(title): continue
            loc = (job.get("location",{}).get("name","") + " " +
                   " ".join(o.get("name","")
                            for o in job.get("offices",[]))).lower()
            pairs = []
            if "hyderabad" in loc and hyd_id:
                pairs.append((hyd_id,"HYD","Hyderabad"))
            if any(x in loc for x in
                   ["bengaluru","bangalore","blr"]) and blr_id:
                pairs.append((blr_id,"BLR","Bengaluru"))
            if not pairs:
                if "india" in loc or not loc.strip():
                    if hyd_id: pairs.append((hyd_id,"HYD","Hyderabad"))
                    if blr_id: pairs.append((blr_id,"BLR","Bengaluru"))
            for gcc_id, city_key, city_d in pairs:
                jid = f"gh_{token}_{city_key}_{job.get('id','')}"
                if jid in seen: continue
                seen.add(jid)
                jobs.append(make_job(
                    gcc_id, company, city_key, city_d, title,
                    job.get("absolute_url",""), job.get("updated_at",""),
                    "greenhouse", jid))
        time.sleep(0.3)
    except Exception:
        pass
    return jobs


# ═══════════════════════════════════════════════════════════════
#  SOURCE 3 — LEVER DIRECT
# ═══════════════════════════════════════════════════════════════
LV_BASE = "https://api.lever.co/v0/postings/{slug}?mode=json"

def fetch_lever(hyd_id, blr_id, company, slug, seen):
    jobs = []
    try:
        r = requests.get(LV_BASE.format(slug=slug), timeout=20)
        if r.status_code != 200: return jobs
        data = r.json()
        if not isinstance(data, list): return jobs
        for job in data:
            title = job.get("text","")
            if not is_security_role(title): continue
            loc = job.get("categories",{}).get("location","").lower()
            pairs = []
            if "hyderabad" in loc and hyd_id:
                pairs.append((hyd_id,"HYD","Hyderabad"))
            if any(x in loc for x in
                   ["bengaluru","bangalore","blr"]) and blr_id:
                pairs.append((blr_id,"BLR","Bengaluru"))
            if not pairs:
                if "india" in loc or not loc:
                    if hyd_id: pairs.append((hyd_id,"HYD","Hyderabad"))
                    if blr_id: pairs.append((blr_id,"BLR","Bengaluru"))
            for gcc_id, city_key, city_d in pairs:
                jid = f"lv_{slug}_{city_key}_{job.get('id','')}"
                if jid in seen: continue
                seen.add(jid)
                ts = job.get("createdAt",0)
                posted = (datetime.fromtimestamp(ts/1000, tz=timezone.utc)
                          .isoformat() if ts else "")
                jobs.append(make_job(
                    gcc_id, company, city_key, city_d, title,
                    job.get("hostedUrl",""), posted, "lever", jid))
        time.sleep(0.3)
    except Exception:
        pass
    return jobs


# ═══════════════════════════════════════════════════════════════
#  SOURCE 4 — JSEARCH FALLBACK (8 calls)
# ═══════════════════════════════════════════════════════════════
def fetch_jsearch_all(seen):
    all_jobs = []
    headers = {"X-RapidAPI-Key": JSEARCH_KEY,
               "X-RapidAPI-Host": JSEARCH_HOST}
    for city in CITIES:
        print(f"\n  JSearch — {city['name']}")
        for q in JSEARCH_QUERIES:
            try:
                r = requests.get(JSEARCH_URL, headers=headers, params={
                    "query": f"{q} in {city['jsearch']}",
                    "num_pages":"1","date_posted":"month",
                    "employment_types":"FULLTIME",
                }, timeout=30)
                r.raise_for_status()
                results = r.json().get("data",[])
                matched = 0
                for job in results:
                    jid = job.get("job_id","")
                    if not jid or jid in seen: continue
                    gcc_id = match_gcc_jsearch(
                        job.get("employer_name",""), city["key"])
                    if not gcc_id: continue
                    seen.add(jid)
                    matched += 1
                    all_jobs.append(make_job(
                        gcc_id, job.get("employer_name",""),
                        city["key"],
                        "Hyderabad" if city["key"]=="HYD" else "Bengaluru",
                        job.get("job_title",""),
                        job.get("job_apply_link",""),
                        job.get("job_posted_at_datetime_utc",""),
                        "jsearch", jid))
                print(f"    {q[:48]}… → {len(results)} returned, "
                      f"{matched} matched")
                time.sleep(0.4)
            except Exception as e:
                print(f"    JSearch error: {e}")
    return all_jobs


# ═══════════════════════════════════════════════════════════════
#  MAIN
# ═══════════════════════════════════════════════════════════════
def main():
    print(f"\n{'='*65}")
    print(f"GCC Multi-Source Fetcher v3 — "
          f"{datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}")
    print(f"Workday: {len(WORKDAY_COMPANIES)} companies  |  "
          f"Greenhouse: {len(GREENHOUSE_COMPANIES)}  |  "
          f"Lever: {len(LEVER_COMPANIES)}  |  JSearch: 8 calls")
    print(f"{'='*65}\n")

    sb = create_client(SUPABASE_URL, SUPABASE_KEY)
    sb.table("gcc_jobs").update({"is_active":False}).neq("id","x").execute()
    print("Marked existing jobs inactive\n")

    all_jobs = []; seen = set()

    # Source 1 — Workday
    print(f"{'─'*40}")
    print(f"SOURCE 1 — Workday ({len(WORKDAY_COMPANIES)} companies)")
    print(f"{'─'*40}")
    wd_total = 0
    for row in WORKDAY_COMPANIES:
        hyd_id,blr_id,company,base_url = row
        jobs = fetch_workday(hyd_id,blr_id,company,base_url,seen)
        if jobs:
            all_jobs.extend(jobs); wd_total+=len(jobs)
            print(f"  ✓ {company}: {len(jobs)} jobs")
        time.sleep(0.5)
    print(f"  → Total: {wd_total}\n")

    # Source 2 — Greenhouse
    print(f"{'─'*40}")
    print(f"SOURCE 2 — Greenhouse ({len(GREENHOUSE_COMPANIES)} companies)")
    print(f"{'─'*40}")
    gh_total = 0
    for hyd_id,blr_id,company,token in GREENHOUSE_COMPANIES:
        jobs = fetch_greenhouse(hyd_id,blr_id,company,token,seen)
        if jobs:
            all_jobs.extend(jobs); gh_total+=len(jobs)
            print(f"  ✓ {company}: {len(jobs)} jobs")
        time.sleep(0.3)
    print(f"  → Total: {gh_total}\n")

    # Source 3 — Lever
    print(f"{'─'*40}")
    print(f"SOURCE 3 — Lever ({len(LEVER_COMPANIES)} companies)")
    print(f"{'─'*40}")
    lv_total = 0
    for hyd_id,blr_id,company,slug in LEVER_COMPANIES:
        jobs = fetch_lever(hyd_id,blr_id,company,slug,seen)
        if jobs:
            all_jobs.extend(jobs); lv_total+=len(jobs)
            print(f"  ✓ {company}: {len(jobs)} jobs")
        time.sleep(0.3)
    print(f"  → Total: {lv_total}\n")

    # Source 4 — JSearch
    print(f"{'─'*40}")
    print(f"SOURCE 4 — JSearch (8 API calls)")
    print(f"{'─'*40}")
    js_jobs = fetch_jsearch_all(seen)
    all_jobs.extend(js_jobs)
    print(f"  → Total: {len(js_jobs)}\n")

    # Save
    print(f"{'─'*40}")
    print(f"💾 Saving {len(all_jobs)} jobs to Supabase…")
    if all_jobs:
        for i in range(0,len(all_jobs),100):
            sb.table("gcc_jobs").upsert(
                all_jobs[i:i+100],on_conflict="id").execute()
    companies = len(set(j["gcc_id"] for j in all_jobs))
    sb.table("gcc_fetch_status").insert({
        "fetched_at":          datetime.now(timezone.utc).isoformat(),
        "total_jobs":          len(all_jobs),
        "companies_with_jobs": companies,
    }).execute()

    print(f"\n{'='*65}")
    print(f"✅ COMPLETE")
    print(f"   Workday:    {wd_total}")
    print(f"   Greenhouse: {gh_total}")
    print(f"   Lever:      {lv_total}")
    print(f"   JSearch:    {len(js_jobs)}")
    print(f"   TOTAL:      {len(all_jobs)} jobs, {companies} companies")
    print(f"{'='*65}\n")

if __name__ == "__main__":
    main()
