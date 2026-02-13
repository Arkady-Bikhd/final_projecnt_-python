import gspread
from oauth2client.service_account import ServiceAccountCredentials

from dotenv import load_dotenv
from os import environ


def write_to_sheet(sheet_data):

    load_dotenv()
    scope = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']
    credentials = ServiceAccountCredentials.from_json_keyfile_name('credentials.json', scope)
    gc = gspread.authorize(credentials)
    spreadsheet_id = environ['SPREADSHEET_ID']
    workbook = gc.open_by_key(spreadsheet_id)    
    sheet = workbook.sheet1 
    try:
        sheet.update(sheet_data, 'A1:B3')        
    except gspread.exceptions.APIError:
        raise
