# Close API Take-Home Project

The project is a script to clean and validate data, create a report, and create new contacts inside Close via their API. You can create a report which pulls from you Close leads to show revenue info by State within a time frame specified by you in either the .env file or by setting it in the command line (more on that below).

To clean the data, data_cleaner.py is called to do things like: 
- Make all of the names follow the same format
- Validate if a provided email is valid using the email-validator python library
- Validate if a phone number is valid using Google's phonenumbers python library
- Cleans the currency symbols from the revenue column
- Checks for existing leads and contacts before creating or updating the new contact information
- Pulls the leads with a company founded in the specified time frame 

To identify data which was invalid, I used a combination of manual review (to identify the types/patterns of issues I need to address), 3rd party python libraries, and regex. 

## Setup
I didn't want to re-number everything, but use a .venv to install by 
```bash
python -m venv .venv
source .venv/bin/activate
```
or don't 

1. Install dependencies:
```bash
pip install -r requirements.txt
```

2. Set up environment variables:
```bash
cp .env.example .env
# Edit .env with your actual values
```

## Usage

The script supports both environment variables (via `.env` file) and command-line arguments. Command-line arguments will override `.env` values.

### Using .env file (default)
```bash
python main.py
```

### Using command-line arguments
```bash
python main.py --api-key "your_key" --file "data.csv" --start-date "1978-01-01" --end-date "2010-12-31"
```

### Mixed approach (.env defaults with command-line overrides)
```bash
# Uses .env values but overrides the API key
python main.py --api-key "different_key"
```

## Environment Variables

- `API_KEY`: Your Close API key (required)
- `FILE`: Path to the CSV file (required)
- `START_DATE`: Start date for filtering (YYYY-MM-DD) (required)
- `END_DATE`: End date for filtering (YYYY-MM-DD) (required)
- `OUTPUT`: Output CSV filename (optional, defaults to `state_segments.csv`)
