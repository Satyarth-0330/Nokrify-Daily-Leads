import os
import json
import time
import requests
from bs4 import BeautifulSoup
from googlesearch import search
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from urllib.parse import urlparse

# Set up logging for GitHub Actions
print("Starting Nokrify Cloud Scraper...")

# Google Sheets Setup
SHEET_ID = os.environ.get("SHEET_ID")
GOOGLE_CRED_JSON = os.environ.get("GOOGLE_CREDENTIALS")

def main():
    if not SHEET_ID or not GOOGLE_CRED_JSON:
        print("Error: Missing Google Sheets credentials in GitHub Secrets.")
        print("Please configure SHEET_ID and GOOGLE_CREDENTIALS.")
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

    # Phase 1: Search for Hiring Companies (Stage 1 of Playbook)
    # Using Google Dorks to find ATS postings (Lever, Greenhouse) for Sales/BPO/Startups in India
    search_queries = [
        'site:jobs.lever.co "India" ("Sales" OR "BPO" OR "Telecaller" OR "Customer Support")',
        'site:boards.greenhouse.io "India" ("Sales" OR "BPO" OR "Telecaller" OR "Customer Support")'
    ]
    
    companies_found = []
    
    print("Searching for hiring companies...")
    for query in search_queries:
        for j in search(query, num_results=10, lang="en", sleep_interval=2):
            # Extract company name from URL
            # e.g., https://jobs.lever.co/companyname or https://boards.greenhouse.io/companyname
            parsed = urlparse(j)
            path_parts = parsed.path.strip('/').split('/')
            company_name = ""
            if "lever.co" in parsed.netloc and len(path_parts) > 0:
                company_name = path_parts[0]
            elif "greenhouse.io" in parsed.netloc and len(path_parts) > 0:
                company_name = path_parts[0]
            
            if company_name and company_name not in [c['name'] for c in companies_found]:
                # Format company name (capitalize, remove hyphens)
                formatted_name = company_name.replace('-', ' ').title()
                companies_found.append({
                    "name": formatted_name,
                    "website": f"https://www.{company_name}.com" if "greenhouse" not in company_name else "",
                    "source_link": j
                })

    print(f"Found {len(companies_found)} potential companies.")

    # Phase 2: Find HR Contacts (Stage 2 of Playbook)
    for company in companies_found:
        print(f"Searching HR for {company['name']}...")
        hr_query = f'site:linkedin.com/in ("HR" OR "Talent Acquisition" OR "Recruiter") "{company["name"]}" "India"'
        hr_linkedin = ""
        hr_name = ""
        
        try:
            for j in search(hr_query, num_results=2, lang="en", sleep_interval=2):
                hr_linkedin = j
                # Guess HR name from URL: linkedin.com/in/john-doe-123
                parts = j.split('/in/')
                if len(parts) > 1:
                    raw_name = parts[1].split('/')[0].split('-')
                    hr_name = " ".join([n.capitalize() for n in raw_name if not any(c.isdigit() for c in n)])
                break
        except Exception as e:
            print(f"Search rate limited or error: {e}")
            break

        company["hr_name"] = hr_name
        company["hr_linkedin"] = hr_linkedin
        company["hr_email"] = f"hr@{company['name'].replace(' ', '').lower()}.com"

    # Phase 3: Update Google Sheet
    # Columns: S.No, Company Name, Industry, Website, Linkedin, Hr name, Hr email, Hr phone, Hr 2 name, Hr 2 email, Hr 2 phone, Source, Location, Status
    if len(companies_found) > 0:
        existing_records = sheet.get_all_records()
        next_sno = len(existing_records) + 1
        
        rows_to_insert = []
        for c in companies_found:
            row = [
                next_sno,
                c['name'],
                "Startup / BPO / Sales", # Industry Guess
                c['website'],
                "", # Company LinkedIn
                c['hr_name'],
                c['hr_email'],
                "", # HR Phone
                "", # HR 2 Name
                "", # HR 2 Email
                "", # HR 2 Phone
                c['source_link'], # Source
                "India", # Location
                "New Lead" # Status
            ]
            rows_to_insert.append(row)
            next_sno += 1
            
        print(f"Adding {len(rows_to_insert)} rows to Google Sheet...")
        sheet.append_rows(rows_to_insert)
        print("Data successfully synced to Google Sheets!")
    else:
        print("No new companies found today to add.")

if __name__ == "__main__":
    main()
