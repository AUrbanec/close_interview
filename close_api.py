import time
import requests

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
                retry_after = float(response.headers.get('Retry-After', 1.0))
                print(f"[!] Rate limited. Sleeping for {retry_after} seconds...")
                time.sleep(retry_after)
                continue
            response.raise_for_status()
            return response.json()

    def get_or_create_custom_field(self, name, field_type):
        """Ensures a custom field exists and returns its ID."""
        data = self.request('GET', '/custom_field/lead/')
        for field in data.get('data', []):
            if field['name'] == name:
                return field['id']
        
        payload = {"name": name, "type": field_type}
        data = self.request('POST', '/custom_field/lead/', json=payload)
        return data['id']

    def find_lead_by_name(self, company_name):
        """Searches for an exact company name match to prevent duplicates."""
        query_str = f'name:"{company_name}"'
        data = self.request('GET', '/lead/', params={'query': query_str, '_limit': 1})
        
        results = data.get('data',[])
        if results and results[0]['name'].lower() == company_name.lower():
            return results[0]
        return None

    def create_lead(self, payload):
        """Creates a new lead."""
        return self.request('POST', '/lead/', json=payload)

    def update_lead(self, lead_id, payload):
        """Updates an existing lead."""
        return self.request('PUT', f'/lead/{lead_id}/', json=payload)

    def create_contact(self, payload):
        """Creates a new contact and attaches it to an existing lead."""
        return self.request('POST', '/contact/', json=payload)

    def search_leads_by_date(self, start_date, end_date, field_id):
        """
        Searches for leads founded within a date range using the API.
        Per Close API Docs: searching by custom field name is deprecated. 
        We MUST use the custom field ID (e.g., custom.lcf_12345).
        """
        leads =[]
        skip = 0
        
        # Correct Close API syntax: custom.cf_XXXX >= "YYYY-MM-DD"
        query_str = f'custom.{field_id} >= "{start_date}" custom.{field_id} <= "{end_date}"'
        print(f"   Debug: Using date range query: {query_str}")
        
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