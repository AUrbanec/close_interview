import re
import datetime
import phonenumbers
from email_validator import validate_email, EmailNotValidError

def normalize_name(name_str):
    """
    Normalizes the contact name so the first letter of each word is capitalized.
    Handles hyphens and apostrophes (e.g., "d'andrea" -> "D'Andrea").
    If name is empty or an email address, returns "Unknown".
    """
    if not name_str or not name_str.strip():
        return "Unknown"
    
    name_str = name_str.strip()
    
    # Check if the name is an email address
    email_pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
    if re.match(email_pattern, name_str):
        return "Unknown"
    
    return name_str.title()

def clean_email(email_str):
    """Splits the email string by delimiters, takes the first element, and validates it."""
    if not email_str:
        return None
    first_part = re.split(r'[;,|\n]', email_str)[0].strip()
    if not first_part:
        return None
    try:
        valid = validate_email(first_part, check_deliverability=False)
        return valid.normalized
    except EmailNotValidError:
        return None

def clean_phone(phone_str):
    """Splits the phone string by delimiters, strips weird characters, and validates."""
    if not phone_str:
        return None
    first_part = re.split(r'[;,|\n]', phone_str)[0].strip()
    first_part = re.sub(r'[^\d\+\-\(\)\s]', '', first_part)
    if not first_part:
        return None
    try:
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
    clean_str = rev_str.replace('$', '').replace(',', '').replace('"', '').strip()
    try:
        return float(clean_str)
    except ValueError:
        return None