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
LOCATIONS = ["Noida", "Delhi", "Gurugram", "Gurgaon", "Delhi NCR", "Meerut"]
TARGET_COUNT = 100

def extract_contact_info(url):
    """Crawls a webpage to extract authentic emails and Indian mobile numbers."""
    try:
        headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)'}
        response = requests.get(url, headers=headers, timeout=8)
        if response.status_code != 200:
            return None, None
            
        text = response.text
        # Regex for emails (exclude images)
        email_pattern = r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}'
        raw_emails = set(re.findall(email_pattern, text))
        valid_emails = [e.lower() for e in raw_emails if not e.endswith(('.png', '.jpg', '.jpeg', '.gif', '.svg', 'sentry.io'))]
        
        # Regex for Indian Mobile Numbers (Must start with 6,7,8,9 and total 10 digits)
        # Extracts 10 digits accurately ignoring +91 formatting
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

def main():
    print("Starting Advanced Nokrify Cloud Scraper (Phase 2)...")
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
        print(f"Error reading sheet. Assuming empty. Error: {e}")
        existing_records = []
        
    existing_companies = {str(row.get('Company Name', '')).strip().lower() for row in existing_records if row.get('Company Name')}
    existing_links = {str(row.get('Source', '')).strip() for row in existing_records if row.get('Source')}
    
    companies_to_add = []
    
    def try_add_lead(name, industry, website, hr_email, hr_phone, source_link, location):
        if len(companies_to_add) >= TARGET_COUNT:
            return False
            
        clean_name = str(name).strip().lower()
        if not clean_name or clean_name in existing_companies:
            return True # Duplicate, but don't stop the loop
        if source_link and source_link in existing_links:
            return True
            
        existing_companies.add(clean_name)
        existing_links.add(source_link)
        
        companies_to_add.append({
            "name": name.title(),
            "industry": industry,
            "website": website,
            "hr_email": hr_email or "",
            "hr_phone": hr_phone or "",
            "source_link": source_link or "",
            "location": location
        })
        print(f"Found new lead: {name.title()} ({len(companies_to_add)}/{TARGET_COUNT})")
        return len(companies_to_add) < TARGET_COUNT


    # ==========================================
    # STRATEGY 1: DDGS Maps (BPOs across locations)
    # ==========================================
    keywords = ["BPO", "Call Center", "Sales Company", "Startup hiring"]
    print("\n--- Starting Maps Search ---")
    with DDGS() as ddgs:
        for loc in LOCATIONS:
            if len(companies_to_add) >= TARGET_COUNT: break
            for kw in keywords:
                if len(companies_to_add) >= TARGET_COUNT: break
                query = f"{kw} in {loc}"
                try:
                    # maps() returns dictionaries with title, address, phone, url
                    results = ddgs.maps(query, max_results=15)
                    if results:
                        for r in results:
                            name = r.get('title')
                            phone = extract_phones_from_text(r.get('phone', ''))
                            website = r.get('url', '')
                            # Attempt to crawl website if no phone
                            email = ""
                            if website and not phone:
                                email, p2 = extract_contact_info(website)
                                if p2: phone = p2
                            
                            # Maps results usually are highly authentic BPOs
                            try_add_lead(name, "BPO/Sales/Startup", website, email, phone, website or f"Maps: {query}", loc)
                            if len(companies_to_add) >= TARGET_COUNT: break
                except Exception as e:
                    print(f"Maps API skip for {query}: {e}")

    # ==========================================
    # STRATEGY 2: Instagram Dorks for HRs
    # ==========================================
    print("\n--- Starting Instagram Dorks ---")
    ig_queries = [
        f'site:instagram.com "Hiring" ("BPO" OR "Telecaller" OR "Sales") "{loc}"' for loc in LOCATIONS
    ]
    with DDGS() as ddgs:
        for query in ig_queries:
            if len(companies_to_add) >= TARGET_COUNT: break
            try:
                results = ddgs.text(query, max_results=10)
                if results:
                    for r in results:
                        link = r.get('href', '')
                        snippet = r.get('body', '') + " " + r.get('title', '')
                        
                        # Extract authentic phone safely from IG post description
                        phone = extract_phones_from_text(snippet)
                        
                        # Guess company name from IG title e.g. "We are hiring! BPO... - @Company"
                        name_match = re.search(r'@(\w+)', snippet)
                        company_name = name_match.group(1) if name_match else "Instagram Post (Unknown)"
                        
                        try_add_lead(company_name, "Instagram Hiring Post", "", "", phone, link, "Delhi NCR Area")
                        if len(companies_to_add) >= TARGET_COUNT: break
            except Exception as e:
                print(f"IG Dork skip for {query}: {e}")

    # ==========================================
    # STRATEGY 3: ATS Crawling (Lever, Greenhouse)
    # ==========================================
    print("\n--- Starting ATS Crawling ---")
    ats_queries = [
        f'site:jobs.lever.co OR site:boards.greenhouse.io ("Sales" OR "BPO" OR "Telecaller") "{loc}"' for loc in LOCATIONS
    ]
    with DDGS() as ddgs:
        for query in ats_queries:
            if len(companies_to_add) >= TARGET_COUNT: break
            try:
                results = ddgs.text(query, max_results=10)
                if results:
                    for r in results:
                        link = r.get('href', '')
                        if not link: continue
                        
                        parsed = urlparse(link)
                        path = parsed.path.strip('/').split('/')
                        company_name = path[0] if path else "ATS Web Lead"
                        
                        # Use our Authenticator Crawler!
                        email, phone = extract_contact_info(link)
                        
                        try_add_lead(company_name, "Tech/Startup Role", f"https://www.{company_name}.com", email, phone, link, "Delhi NCR Area")
                        if len(companies_to_add) >= TARGET_COUNT: break
            except Exception as e:
                print(f"ATS crawl skip for {query}: {e}")


    # ==========================================
    # FINAL PHASE: Insert to Google Sheets
    # ==========================================
    if len(companies_to_add) > 0:
        next_sno = len(existing_records) + 1
        rows_to_insert = []
        for c in companies_to_add:
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
                "New Cloud Lead"                    # Status
            ]
            rows_to_insert.append(row)
            next_sno += 1
            
        print(f"\nAdding {len(rows_to_insert)} new deduplicated leads to Google Sheet...")
        sheet.append_rows(rows_to_insert)
        print("Data successfully synced to Google Sheets!")
    else:
        print("\nNo new companies found today to add.")

if __name__ == "__main__":
    main()
