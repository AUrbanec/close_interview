import csv
import json
import re
import time
import argparse
import datetime
import statistics
import requests
import phonenumbers
from email_validator import validate_email, EmailNotValidError

# Close API Base URL
BASE_URL = "https://api.close.com/api/v1"

class CloseAPI:
    """Helper class to interact with the Close API, handling rate limits and auth."""
    def __init__(self, api_key):
        self.api_key = api_key
        self.auth = (self.api_key, '')

    def request(self, method, endpoint, **kwargs):
        """Executes an API request and gracefully handles 429 Rate Limits."""
        url = f"{BASE_URL}{endpoint}"
        while True:
            response = requests.request(method, url, auth=self.auth, **kwargs)
            if response.status_code == 429:
                # Close API returns 'Retry-After' in seconds
                retry_after = float(response.headers.get('Retry-After', 1.0))
                print(f"Rate limited. Sleeping for {retry_after} seconds...")
                time.sleep(retry_after)
                continue
            response.raise_for_status()
            return response.json()

    def get_or_create_custom_field(self, name, field_type):
        """
        Ensures a custom field exists. The Close API requires using the custom field's ID 
        (e.g., custom.lcf_12345) rather than its name for robust data insertion.
        """
        # Fetch all Lead Custom Fields
        data = self.request('GET', '/custom_field/lead/')
        for field in data.get('data', []):
            if field['name'] == name:
                return field['id']
        
        # Create it if it doesn't exist
        payload = {"name": name, "type": field_type}
        data = self.request('POST', '/custom_field/lead/', json=payload)
        return data['id']

    def create_lead(self, payload):
        """Creates a single lead in Close."""
        return self.request('POST', '/lead/', json=payload)

    def search_leads_by_date(self, start_date, end_date):
        """
        Uses the Close API search query parameter to find leads founded within a date range.
        Handles pagination automatically.
        """
        leads =[]
        skip = 0
        # The query syntax matches the Close UI search bar
        query_str = f'custom."Company Founded" >= "{start_date}" custom."Company Founded" <= "{end_date}"'
        
        while True:
            data = self.request('GET', '/lead/', params={
                'query': query_str,
                '_limit': 100,
                '_skip': skip
            })
            leads.extend(data.get('data',[]))
            
            if not data.get('has_more'):
                break
            skip += 100
            
        return leads


# --- Data Cleaning & Validation Functions ---

def clean_email(email_str):
    """
    Splits the email string by delimiters. As noted in the manual review, multiple 
    emails in a single field are errors bleeding over from adjacent rows. Therefore, 
    we ONLY take the first element and validate it.
    """
    if not email_str:
        return None
    # Split by common delimiters: comma, semicolon, newline
    first_part = re.split(r'[;,|\n]', email_str)[0].strip()
    if not first_part:
        return None
    try:
        valid = validate_email(first_part, check_deliverability=False)
        return valid.normalized
    except EmailNotValidError:
        return None

def clean_phone(phone_str):
    """
    Splits the phone string by delimiters, taking only the first element to avoid 
    bleed-over errors. Strips weird characters and validates using phonenumbers.
    """
    if not phone_str:
        return None
    first_part = re.split(r'[;,|\n]', phone_str)[0].strip()
    # Remove any non-phone characters (like the phone emoji or question marks)
    first_part = re.sub(r'[^\d\+\-\(\)\s]', '', first_part)
    if not first_part:
        return None
    try:
        # If no '+' is provided, default to US region parsing
        region = "US" if not first_part.startswith('+') else None
        parsed = phonenumbers.parse(first_part, region)
        if phonenumbers.is_valid_number(parsed):
            return phonenumbers.format_number(parsed, phonenumbers.PhoneNumberFormat.E164)
    except phonenumbers.NumberParseException:
        pass
    return None

def parse_date(date_str):
    """Parses DD.MM.YYYY into Close API's required YYYY-MM-DD format."""
    if not date_str:
        return None
    try:
        dt = datetime.datetime.strptime(date_str.strip(), "%d.%m.%Y")
        return dt.strftime("%Y-%m-%d")
    except ValueError:
        return None

def parse_revenue(rev_str):
    """Cleans currency strings into standard floats."""
    if not rev_str:
        return None
    # Remove $, commas, quotes, and whitespace
    clean_str = rev_str.replace('$', '').replace(',', '').replace('"', '').strip()
    try:
        return float(clean_str)
    except ValueError:
        return None


# --- Main Execution Flow ---

def main():
    parser = argparse.ArgumentParser(description="Close API Take-Home Project")
    parser.add_argument("--api-key", required=True, help="Your Close API Key")
    parser.add_argument("--file", required=True, help="Path to the MOCK_DATA.csv file")
    parser.add_argument("--start-date", required=True, help="Start date for filtering (YYYY-MM-DD)")
    parser.add_argument("--end-date", required=True, help="End date for filtering (YYYY-MM-DD)")
    parser.add_argument("--output", default="state_segments.csv", help="Output CSV filename")
    args = parser.parse_args()

    api = CloseAPI(args.api_key)

    print("1. Setting up Custom Fields in Close...")
    founded_cf_id = api.get_or_create_custom_field("Company Founded", "date")
    revenue_cf_id = api.get_or_create_custom_field("Company Revenue", "number")

    print("2. Parsing and grouping CSV data...")
    leads_map = {}
    
    with open(args.file, mode='r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            company = row.get('Company', '').strip()
            if not company:
                continue # Skip rows with completely blank company names
            
            # Initialize the lead if it's the first time we've seen this company
            if company not in leads_map:
                leads_map[company] = {
                    "name": company,
                    "contacts":[],
                    "founded": parse_date(row.get('custom.Company Founded')),
                    "revenue": parse_revenue(row.get('custom.Company Revenue')),
                    "state": row.get('Company US State', '').strip()
                }
            
            # Process the contact for this row
            contact_name = row.get('Contact Name', '').strip()
            email = clean_email(row.get('Contact Emails', ''))
            phone = clean_phone(row.get('Contact Phones', ''))
            
            contact_obj = {}
            if contact_name: contact_obj["name"] = contact_name
            if email: contact_obj["emails"] = [{"email": email}]
            if phone: contact_obj["phones"] = [{"phone": phone}]
            
            # Only append if there is actual contact data
            if contact_obj:
                leads_map[company]["contacts"].append(contact_obj)

    print(f"3. Pushing {len(leads_map)} grouped Leads to Close...")
    for company_name, lead_data in leads_map.items():
        payload = {
            "name": lead_data["name"],
            "contacts": lead_data["contacts"]
        }
        # Attach custom fields using their specific Close IDs
        if lead_data["founded"]:
            payload[f"custom.{founded_cf_id}"] = lead_data["founded"]
        if lead_data["revenue"] is not None:
            payload[f"custom.{revenue_cf_id}"] = lead_data["revenue"]
        # Attach state via the standard addresses array
        if lead_data["state"]:
            payload["addresses"] = [{"state": lead_data["state"], "country": "US"}]
            
        api.create_lead(payload)

    print(f"4. Fetching leads founded between {args.start_date} and {args.end_date}...")
    filtered_leads = api.search_leads_by_date(args.start_date, args.end_date)
    print(f"   Found {len(filtered_leads)} leads matching the date range.")

    print("5. Segmenting by US State and calculating metrics...")
    state_metrics = {}
    
    for lead in filtered_leads:
        # Safely extract state from the addresses array
        state = "Unknown"
        addresses = lead.get('addresses',[])
        if addresses and addresses[0].get('state'):
            state = addresses[0]['state']
            
        # Safely extract revenue
        revenue = lead.get(f"custom.{revenue_cf_id}", 0)
        if revenue is None: 
            revenue = 0
            
        if state not in state_metrics:
            state_metrics[state] = {
                "count": 0,
                "revenues":[],
                "top_lead_name": None,
                "top_lead_revenue": -1
            }
            
        stats = state_metrics[state]
        stats["count"] += 1
        stats["revenues"].append(revenue)
        
        # Track the lead with the highest revenue in this state
        if revenue > stats["top_lead_revenue"]:
            stats["top_lead_revenue"] = revenue
            stats["top_lead_name"] = lead.get("display_name", "Unknown")

    print(f"6. Generating output CSV: {args.output}")
    with open(args.output, mode='w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow([
            "State", 
            "Total Leads", 
            "Lead with Most Revenue", 
            "Total Revenue", 
            "Median Revenue"
        ])
        
        for state, stats in state_metrics.items():
            total_rev = sum(stats["revenues"])
            median_rev = statistics.median(stats["revenues"]) if stats["revenues"] else 0
            
            writer.writerow([
                state,
                stats["count"],
                stats["top_lead_name"],
                f"${total_rev:,.2f}",
                f"${median_rev:,.2f}"
            ])
            
    print("Done! Project execution completed successfully.")

if __name__ == "__main__":
    main()