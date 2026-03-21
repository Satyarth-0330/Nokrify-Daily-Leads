import os
import json
import time
import requests
import re
from bs4 import BeautifulSoup
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from urllib.parse import urlparse

# Settings
SHEET_ID = os.environ.get("SHEET_ID")
GOOGLE_CRED_JSON = os.environ.get("GOOGLE_CREDENTIALS")
SCRAPINGDOG_API_KEY = os.environ.get("SCRAPINGDOG_API_KEY", "69be41b5af72e60060716a82")

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
    """Extracts mobile numbers from raw text (like snippets)."""
    if not text:
        return None
    phone_pattern = r'(?:\+91[-.\s]?)?([6789]\d{9})'
    phones = re.findall(phone_pattern, text)
    return phones[0] if phones else None

def is_consultancy(text):
    if not text: return False
    text_lower = text.lower()
    return any(word in text_lower for word in BLACKLIST)

def passes_location_filter(text):
    if not text: return False
    text_lower = text.lower()
    return any(loc.lower() in text_lower for loc in LOCATIONS)

def dog_search(query, is_maps=False):
    """Calls the ScrapingDog API directly for Google Search or Maps."""
    endpoint = "google_maps" if is_maps else "google"
    url = f"https://api.scrapingdog.com/{endpoint}"
    params = {
        "api_key": SCRAPINGDOG_API_KEY,
        "query": query,
        "results": 15 if is_maps else 10,
        "country": "in"
    }
    
    try:
        response = requests.get(url, params=params, timeout=30)
        if response.status_code == 200:
            data = response.json()
            if is_maps:
                return data.get("search_results", []) or data.get("local_results", [])
            else:
                return data.get("organic_data", [])
        else:
            print(f"ScrapingDog Error: {response.text}")
            return []
    except Exception as e:
        print(f"ScrapingDog Request Failed: {e}")
        return []

def main():
    print("Starting Nokrify Ultimate Cloud Scraper (ScrapingDog Powered)...")
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
            return True
        if source_link and source_link in existing_links:
            return True
            
        combined_text = f"{name} {website} {source_link} {snippet}".lower()
        if is_consultancy(combined_text):
            print(f"Dropped Consultancy: {name}")
            return True
            
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
        print(f"✅ Found Lead: {name.title()} ({len(companies_to_add)}/{TARGET_COUNT})")
        return len(companies_to_add) < TARGET_COUNT


    # ==========================================
    # STAGE 1: Maps API (BPOs & Call Centers, Direct phone extraction)
    # ==========================================
    print("\n--- Starting ScrapingDog Google Maps Search ---")
    map_keywords = ["BPO", "International Call Center"]
    for loc in LOCATIONS:
        if len(companies_to_add) >= TARGET_COUNT: break
        for kw in map_keywords:
            if len(companies_to_add) >= TARGET_COUNT: break
            query = f"{kw} {loc}"
            
            results = dog_search(query, is_maps=True)
            for r in results:
                name = r.get('title') or r.get('name', '')
                phone = extract_phones_from_text(r.get('phone', ''))
                website = r.get('website') or r.get('url', '')
                address = r.get('address', '')
                
                email = ""
                if website and not phone:
                    email, p2 = extract_contact_info(website)
                    if p2: phone = p2
                
                # Maps natively provides highly accurate direct business locations
                try_add_lead(name, "BPO/Call Center", website, email, phone, website or f"Maps: {query}", address, snippet=address)
                if len(companies_to_add) >= TARGET_COUNT: break

    # ==========================================
    # STAGE 2: Instagram Vacancy Scraping
    # ==========================================
    print("\n--- Starting ScrapingDog Instagram Search ---")
    ig_queries = [
        f'site:instagram.com ("Hiring" OR "Vacancy") "BPO" "{loc}"' for loc in LOCATIONS
    ]
    for query in ig_queries:
        if len(companies_to_add) >= TARGET_COUNT: break
        # We append 'after:2026-01-01' trick directly in google search query for recency if needed, but
        # ScrapingDog is fresh enough.
        results = dog_search(query)
        for r in results:
            link = r.get('link', '')
            snippet = str(r.get('snippet', '')) + " " + str(r.get('title', ''))
            
            phone = extract_phones_from_text(snippet)
            
            # Guess company from IG handle
            name_match = re.search(r'@(\w+)', snippet)
            company_name = name_match.group(1) if name_match else "Instagram HR Post"
            
            email, p2 = extract_contact_info(link)
            if p2 and not phone: phone = p2
                
            try_add_lead(company_name, "Instagram Hiring", "", email, phone, link, "Delhi NCR Area", snippet=snippet)
            if len(companies_to_add) >= TARGET_COUNT: break

    # ==========================================
    # STAGE 3: Job Portals Reverse Research (Naukri/Apna)
    # ==========================================
    print("\n--- Starting ScrapingDog Job Portals Crawl ---")
    job_queries = [
        f'site:naukri.com/job-listings "BPO" "HR" "{loc}"' for loc in LOCATIONS
    ] + [
        f'site:apna.co/job "BPO" "{loc}"' for loc in LOCATIONS
    ]
    for query in job_queries:
        if len(companies_to_add) >= TARGET_COUNT: break
        # Google Dork: 'qdr:w' means past week, 'qdr:d' means past day
        fresh_query = f"{query} qdr:w" 
        results = dog_search(fresh_query)
        for r in results:
            link = r.get('link', '')
            snippet = str(r.get('snippet', '')) + " " + str(r.get('title', ''))
            
            name = "Job Portal Lead"
            if "naukri.com" in link:
                parts = link.split('-')
                name = parts[-3] if len(parts) > 3 else name
                
            phone = extract_phones_from_text(snippet)
            email, p2 = extract_contact_info(link)
            if p2 and not phone: phone = p2
                
            try_add_lead(name, "Job Portal Hiring", "", email, phone, link, "Delhi NCR Area", snippet=snippet)
            if len(companies_to_add) >= TARGET_COUNT: break
            
    # ==========================================
    # STAGE 4: LinkedIn HR Discovery
    # ==========================================
    print("\n--- Starting ScrapingDog LinkedIn Search ---")
    linkedin_queries = [
        f'site:linkedin.com/in ("HR" OR "Talent Acquisition") "BPO" "{loc}"' for loc in LOCATIONS
    ]
    for query in linkedin_queries:
        if len(companies_to_add) >= TARGET_COUNT: break
        results = dog_search(query)
        for r in results:
            link = r.get('link', '')
            snippet = str(r.get('snippet', '')) + " " + str(r.get('title', ''))
            
            # Extract name from LinkedIn title
            name_part = str(r.get('title', '')).split('-')[0].strip()
            
            try_add_lead(name_part, "LinkedIn HR Profile", "", "", extract_phones_from_text(snippet), link, "Delhi NCR Area", snippet=snippet)
            if len(companies_to_add) >= TARGET_COUNT: break


    # ==========================================
    # FINAL PHASE: Insert to Google Sheets
    # ==========================================
    if len(companies_to_add) > 0:
        next_sno = len(existing_records) + 1
        rows_to_insert = []
        for c in companies_to_add:
            status = "New Filtered ScrapingDog Lead"
            if not c['hr_email'] and not c['hr_phone']:
                status = "Contact Missing (Check Source)"
                
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
