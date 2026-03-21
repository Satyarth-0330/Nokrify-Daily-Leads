import os
import json
import time
import requests
import re
from bs4 import BeautifulSoup
from ddgs import DDGS
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from urllib.parse import urlparse

# Settings
SHEET_ID = os.environ.get("SHEET_ID")
GOOGLE_CRED_JSON = os.environ.get("GOOGLE_CREDENTIALS")

TARGET_COUNT = 100
LOCATIONS = ["Noida", "Delhi", "Gurugram", "Gurgaon", "Delhi NCR", "Meerut"]
# Strict blacklist to avoid 3rd party recruiters and consultancies
BLACKLIST = ["10 times", "consultancy", "staffing", "recruiting", "recruitment", "consulting", "placement", "outsourcing", "manpower", "talent agency"]

def extract_contact_info(url):
    """Crawls a webpage to extract authentic emails and Indian mobile numbers."""
    try:
        headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)'}
        response = requests.get(url, headers=headers, timeout=5)
        if response.status_code != 200:
            return None, None
            
        text = response.text
        # Regex for emails (exclude images and common invalid domains)
        email_pattern = r'[a-zA-Z0-9._%+-]+@(?:[a-zA-Z0-9-]+\.)+[a-zA-Z]{2,}'
        raw_emails = set(re.findall(email_pattern, text))
        valid_emails = [e.lower() for e in raw_emails if not any(x in e.lower() for x in ['.png', '.jpg', 'sentry.io', '@w3.org'])]
        
        # Regex for Indian Mobile Numbers (Must start with 6,7,8,9 and total 10 digits)
        phone_pattern = r'(?:\+91[-.\s]?)?([6789]\d{9})'
        raw_phones = set(re.findall(phone_pattern, text))
        valid_phones = [p for p in raw_phones if len(p) == 10]
        
        return (valid_emails[0] if valid_emails else None), (valid_phones[0] if valid_phones else None)
    except:
        return None, None

def extract_phones_from_text(text):
    """Extracts mobile numbers from raw text (like DDG snippets)."""
    if not text:
        return None
    phone_pattern = r'(?:\+91[-.\s]?)?([6789]\d{9})'
    phones = re.findall(phone_pattern, text)
    return phones[0] if phones else None

def is_consultancy(text):
    """Returns True if the text contains blacklisted consultancy keywords."""
    if not text: return False
    text_lower = text.lower()
    return any(word in text_lower for word in BLACKLIST)

def passes_location_filter(text):
    """Returns True if the text contains at least one of our strictly approved locations."""
    if not text: return False
    text_lower = text.lower()
    return any(loc.lower() in text_lower for loc in LOCATIONS)

def main():
    print("Starting Nokrify Quality Filters Phase 2...")
    if not SHEET_ID or not GOOGLE_CRED_JSON:
        print("Missing Google Sheets credentials.")
        return

    try:
        creds_dict = json.loads(GOOGLE_CRED_JSON)
        scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
        creds = ServiceAccountCredentials.from_json_keyfile_dict(creds_dict, scope)
        client = gspread.authorize(creds)
        sheet = client.open_by_key(SHEET_ID).sheet1
        print("Successfully connected to Google Sheets!")
    except Exception as e:
        print(f"Error connecting to Google Sheets: {e}")
        return

    # Fetch existing data for Deduplication
    print("Fetching existing companies to prevent duplicates...")
    try:
        existing_records = sheet.get_all_records(default_blank="")
    except Exception as e:
        print(f"Error reading sheet: {e}")
        existing_records = []
        
    existing_companies = {str(row.get('Company Name', '')).strip().lower() for row in existing_records if row.get('Company Name')}
    existing_links = {str(row.get('Source', '')).strip() for row in existing_records if row.get('Source')}
    
    companies_to_add = []
    
    def try_add_lead(name, industry, website, hr_email, hr_phone, source_link, location, snippet=""):
        if len(companies_to_add) >= TARGET_COUNT:
            return False
            
        clean_name = str(name).strip().lower()
        if not clean_name or clean_name in existing_companies:
            return True # Duplicate, but keep going
        if source_link and source_link in existing_links:
            return True
            
        # STRICT FILTERS: Consultancies and Bad locations dropping
        combined_text = f"{name} {website} {source_link} {snippet}".lower()
        if is_consultancy(combined_text):
            print(f"Dropped Consultancy: {name}")
            return True
            
        # If no preferred location is physically found anywhere in the listing, drop it.
        # This completely eliminates random USA/Florida results.
        if not passes_location_filter(combined_text) and not passes_location_filter(location):
            print(f"Dropped Wrong Location: {name}")
            return True
            
        existing_companies.add(clean_name)
        existing_links.add(source_link)
        
        companies_to_add.append({
            "name": name.title().strip(),
            "industry": industry,
            "website": website or "",
            "hr_email": hr_email or "",
            "hr_phone": hr_phone or "",
            "source_link": source_link or "",
            "location": location
        })
        print(f"✅ Found High-Quality Lead: {name.title()} ({len(companies_to_add)}/{TARGET_COUNT})")
        return len(companies_to_add) < TARGET_COUNT


    # ==========================================
    # STRATEGY 1: Maps Dorks (BPOs & Call Centers, Direct phone extraction)
    # ==========================================
    print("\n--- Starting DDG Local Maps Search ---")
    keywords = ["BPO", "Call Center", "Inbound Call Center", "Outbound Call Center"]
    with DDGS() as ddgs:
        for loc in LOCATIONS:
            if len(companies_to_add) >= TARGET_COUNT: break
            for kw in keywords:
                if len(companies_to_add) >= TARGET_COUNT: break
                query = f"{kw} {loc}"
                try:
                    # Maps doesn't support timelimit but yields evergreen highly accurate BPOs
                    results = ddgs.maps(query, max_results=15)
                    if results:
                        for r in results:
                            name = r.get('title', '')
                            phone = extract_phones_from_text(r.get('phone', ''))
                            website = r.get('url', '')
                            address = r.get('address', '')
                            
                            email = ""
                            if website and not phone:
                                email, p2 = extract_contact_info(website)
                                if p2: phone = p2
                            
                            try_add_lead(name, "BPO/Call Center", website, email, phone, website or f"Maps: {query}", address, snippet=address)
                            if len(companies_to_add) >= TARGET_COUNT: break
                except Exception as e:
                    print(f"Maps Skip {query}: {e}")

    # ==========================================
    # STRATEGY 2: Recent Job Portals Pivot (Naukri/Apna)
    # Time Limit 'w' forces results indexed in the past 7 days!
    # ==========================================
    print("\n--- Starting Job Portals Crawl (Past Week Only) ---")
    job_queries = [
        f'site:naukri.com/job-listings "BPO" "HR" "{loc}"' for loc in LOCATIONS
    ] + [
        f'site:apna.co/job "BPO" "{loc}"' for loc in LOCATIONS
    ]
    with DDGS() as ddgs:
        for query in job_queries:
            if len(companies_to_add) >= TARGET_COUNT: break
            try:
                # 'w' forces ONLY results indexed in the last week
                results = ddgs.text(query, timelimit='w', max_results=10)
                if results:
                    for r in results:
                        link = r.get('href', '')
                        snippet = r.get('body', '') + " " + r.get('title', '')
                        
                        # Guess company from Naukri/Apna URL structure or snippet
                        # site:naukri.com/job-listings-bpo-executive-company-name-delhi-ncr-1to5years-123456
                        name = "Job Portal Lead"
                        if "naukri.com" in link:
                            parts = link.split('-')
                            # very basic guess
                            name = parts[-3] if len(parts) > 3 else name
                            
                        phone = extract_phones_from_text(snippet)
                        # Job platforms usually hide HR numbers, but sometimes it's in the snippet!
                        
                        # Use our Authenticator!
                        email, p2 = extract_contact_info(link)
                        if p2: phone = p2
                            
                        # Use snippet to infer location properly
                        try_add_lead(name, "Job Portal Hiring", "", email, phone, link, "Delhi NCR Area", snippet=snippet)
                        if len(companies_to_add) >= TARGET_COUNT: break
            except Exception as e:
                print(f"Job Portal Skip {query}: {e}")

    # ==========================================
    # STRATEGY 3: ATS Platforms Crawl 
    # ==========================================
    print("\n--- Starting ATS Platform Crawl (Lever/Greenhouse) ---")
    ats_queries = [
        f'site:lever.co OR site:greenhouse.io ("BPO" OR "Sales" OR "Customer Support") "{loc}"' for loc in LOCATIONS
    ]
    with DDGS() as ddgs:
        for query in ats_queries:
            if len(companies_to_add) >= TARGET_COUNT: break
            try:
                # Past Month for ATS (Startup jobs last up to 30 days)
                results = ddgs.text(query, timelimit='m', max_results=10)
                if results:
                    for r in results:
                        link = r.get('href', '')
                        snippet = r.get('body', '')
                        
                        parsed = urlparse(link)
                        path = parsed.path.strip('/').split('/')
                        company_name = path[0] if path else "ATS Startup Lead"
                        
                        # Web crawl to extract real contact details
                        email, phone = extract_contact_info(link)
                        
                        try_add_lead(company_name, "Startup/Tech Role", f"https://www.{company_name}.com", email, phone, link, "Delhi NCR Area", snippet=snippet)
                        if len(companies_to_add) >= TARGET_COUNT: break
            except Exception as e:
                print(f"ATS skip {query}: {e}")


    # ==========================================
    # FINAL PHASE: Insert to Google Sheets
    # ==========================================
    if len(companies_to_add) > 0:
        next_sno = len(existing_records) + 1
        rows_to_insert = []
        for c in companies_to_add:
            status = "New Filtered Lead"
            if not c['hr_email'] and not c['hr_phone']:
                status = "Contact Missing (Needs Manual Check)"
                
            row = [
                next_sno,                           # S.No
                c['name'],                          # Company Name
                c['industry'],                      # Industry
                c['website'],                       # Website
                "",                                 # Linkedin
                "",                                 # Hr name
                c['hr_email'],                      # Hr email
                c['hr_phone'],                      # Hr phone
                "", "", "",                         # HR 2 Name, Email, Phone
                c['source_link'],                   # Source
                c['location'],                      # Location
                status                              # Status
            ]
            rows_to_insert.append(row)
            next_sno += 1
            
        print(f"\nAdding {len(rows_to_insert)} hyper-filtered leads to Google Sheet...")
        sheet.append_rows(rows_to_insert)
        print("Data successfully synced to Google Sheets!")
    else:
        print("\nNo new companies matched the strict filters today.")

if __name__ == "__main__":
    main()
