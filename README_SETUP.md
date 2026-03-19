# Nokrify Cloud Scraper Setup Guide

Aapki Cloud Automation ki scripts ready hain! Is folder me saare required files hain.
Inko apne **existing GitHub account** pe upload karke chalane ke liye, yeh 3 steps follow karein:

## STEP 1: Google Sheet Setup karein
1. Ek nayi Google Sheet banayein aur usme columns add karein: `S.No`, `Company Name`, `Industry`, `Website`, `Linkedin`, `Hr name`, `Hr email`, `Hr phone`, `Hr 2 name`, `Hr 2 email`, `Hr 2 phone`, `Source`, `Location`, `Status`.
2. Sheet ka **ID** copy karein. (ID URL me hota hai, jaise: `docs.google.com/spreadsheets/d/YEH_WALA_HOTA_HAI_ID/edit`)

## STEP 2: Google Service Account Banayein (Taki script automatically sheet me likh sake)
1. Browser me open karein: [Google Cloud Console](https://console.cloud.google.com/)
2. Ek "New Project" banayein, naam de: "Nokrify Scraper".
3. Upar search bar me **Google Sheets API** search karein aur "Enable" pe click karein.
4. Left menu me "APIs & Services > Credentials" me jayein.
5. "Create Credentials" > "Service Account" par click karein. Naam `"nokrify-bot"` de dein.
6. Service account banne ke baad uski email ID copy kar lein (jaise: `nokrify-bot@nokrify-scraper.iam.gserviceaccount.com`).
7. Apni **Google Sheet** wapas open karein aur top-right par "Share" daba kar iss naye Email ID ko **Editor** permission de dein.
8. Cloud Console me Service account par wapas click karein, "Keys" tab me jayein > "Add Key" > "Create new key" > **JSON** select karein. Ek file download ho jayegi. Ise safe rakhein.

## STEP 3: GitHub Pe Repo Banayein aur Secrets Add karein
1. Apna GitHub kholiye aur usme ek Nayi Repository banayein (Naam jaise `Nokrify-Daily-Leads`).
2. Is folder `Company Lead Automation` me jitne bhi files hain (paanch file/folders: `.github`, `nokrify_cloud_scraper.py`, `requirements.txt`, `README_SETUP.md`), un sabko us Github repo me Upload kar dein.
3. Github Repo ke "Settings" > "Secrets and variables" > "Actions" me jayein.
4. "New repository secret" pe click karein.
   - **Name**: `SHEET_ID`
   - **Secret**: (Yaha par Step 1 me jo Google Sheet ID copy kiya tha vo paste karein). Click "Add".
5. Kaha se dujara "New repository secret" click karein:
   - **Name**: `GOOGLE_CREDENTIALS`
   - **Secret**: (Jo JSON file Step 2 me download hui thi, usko notepad me khol kar uska sab kuch copy karke yaha paste kar dein). Click "Add".

BAM! 🎉 Aapka Setup pure tarike se ready hai. Ab ye script har din subah 9:00 AM (IST) aapke liye nai leads find karke directly sheet me daal degi! 
Apna pehla run turant check karne ke liye: GitHub Repo me **Actions** tab par jayein -> "Nokrify Daily Lead Scraper" par click karein -> aur **"Run workflow"** daba dein!
