import csv
import time
import argparse
import statistics
from close_api import CloseAPI
from data_cleaner import (
    normalize_name, clean_email, clean_phone, 
    parse_date, parse_revenue
)

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
    # This correctly fetches the cf_xxxx IDs
    founded_cf_id = api.get_or_create_custom_field("Company Founded", "date")
    revenue_cf_id = api.get_or_create_custom_field("Company Revenue", "number")

    print("2. Parsing and grouping CSV data...")
    leads_map = {}
    
    with open(args.file, mode='r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            company = row.get('Company', '').strip()
            if not company:
                continue 
            
            if company not in leads_map:
                leads_map[company] = {
                    "name": company,
                    "contacts":[],
                    "founded": parse_date(row.get('custom.Company Founded')),
                    "revenue": parse_revenue(row.get('custom.Company Revenue')),
                    "state": row.get('Company US State', '').strip()
                }
            
            contact_name = normalize_name(row.get('Contact Name', '').strip())
            email = clean_email(row.get('Contact Emails', ''))
            phone = clean_phone(row.get('Contact Phones', ''))
            
            contact_obj = {}
            if contact_name: contact_obj["name"] = contact_name
            if email: contact_obj["emails"] = [{"email": email}]
            if phone: contact_obj["phones"] = [{"phone": phone}]
            
            if contact_obj:
                leads_map[company]["contacts"].append(contact_obj)

    print(f"3. Upserting {len(leads_map)} grouped Leads to Close (Deduplication Enabled)...")
    for company_name, lead_data in leads_map.items():
        existing_lead = api.find_lead_by_name(company_name)
        
        if existing_lead:
            # --- UPDATE EXISTING LEAD ---
            lead_id = existing_lead['id']
            update_payload = {}
            
            if lead_data["founded"]:
                update_payload[f"custom.{founded_cf_id}"] = lead_data["founded"]
            if lead_data["revenue"] is not None:
                update_payload[f"custom.{revenue_cf_id}"] = lead_data["revenue"]
            if lead_data["state"]:
                update_payload["addresses"] =[{"state": lead_data["state"], "country": "US"}]
                
            if update_payload:
                api.update_lead(lead_id, update_payload)
            
            # --- DEDUPLICATE & ADD CONTACTS (By Name OR Email) ---
            existing_emails = set()
            existing_names = set()
            for existing_contact in existing_lead.get('contacts',[]):
                existing_names.add(existing_contact.get('display_name', '').lower())
                for e in existing_contact.get('emails',[]):
                    existing_emails.add(e.get('email', '').lower())
            
            for new_contact in lead_data["contacts"]:
                contact_email = new_contact.get("emails", [{}])[0].get("email", "").lower() if new_contact.get("emails") else ""
                contact_name = new_contact.get("name", "").lower()
                
                is_duplicate = False
                if contact_email and contact_email in existing_emails:
                    is_duplicate = True
                if contact_name and contact_name in existing_names:
                    is_duplicate = True
                    
                if not is_duplicate:
                    new_contact["lead_id"] = lead_id
                    api.create_contact(new_contact)
                    # Add to sets so we don't duplicate within the same CSV run
                    if contact_email: existing_emails.add(contact_email)
                    if contact_name: existing_names.add(contact_name)
                    
        else:
            # --- CREATE NEW LEAD ---
            payload = {
                "name": lead_data["name"],
                "contacts": lead_data["contacts"]
            }
            if lead_data["founded"]:
                payload[f"custom.{founded_cf_id}"] = lead_data["founded"]
            if lead_data["revenue"] is not None:
                payload[f"custom.{revenue_cf_id}"] = lead_data["revenue"]
            if lead_data["state"]:
                payload["addresses"] = [{"state": lead_data["state"], "country": "US"}]
                
            api.create_lead(payload)

    print("4. Waiting 10 seconds for Close to index the new leads before searching...")
    time.sleep(10)

    print(f"5. Fetching leads founded between {args.start_date} and {args.end_date}...")
    # FIX: Pass the dynamically fetched custom field ID instead of the string "Company Founded"
    filtered_leads = api.search_leads_by_date(args.start_date, args.end_date, founded_cf_id)
    print(f"   Found {len(filtered_leads)} leads matching the date range.")

    print("6. Segmenting by US State and calculating metrics...")
    state_metrics = {}
    
    for lead in filtered_leads:
        state = "Unknown"
        addresses = lead.get('addresses', [])
        if addresses and addresses[0].get('state'):
            state = addresses[0]['state']
            
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
        
        if revenue > stats["top_lead_revenue"]:
            stats["top_lead_revenue"] = revenue
            stats["top_lead_name"] = lead.get("display_name", lead.get("name", "Unknown"))

    print(f"7. Generating output CSV: {args.output}")
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