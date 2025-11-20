import gspread
from oauth2client.service_account import ServiceAccountCredentials

def connect_sheet():
    # Define scope
    scope = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive"
    ]

    # Load credentials from the JSON file you uploaded
    creds = ServiceAccountCredentials.from_json_keyfile_name("credentials.json", scope)

    # Authorize the client
    client = gspread.authorize(creds)

    # Open your sheet by name
    sheet = client.open("BlitzDealTracker").sheet1

    return sheet

def add_deal(blitz_name, user, message):
    sheet = connect_sheet()

    # Append a row: BlitzName | User | Message | Timestamp
    sheet.append_row([blitz_name, user, message])
