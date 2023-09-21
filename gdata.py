import os
import pickle
import pandas as pd
import json
from pymongo import MongoClient
from dotenv import load_dotenv
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request

# Load environment variables
load_dotenv()

# If modifying these SCOPES, delete the file token.pickle.
SCOPES = ['https://www.googleapis.com/auth/spreadsheets.readonly']

# The ID and range of a sample spreadsheet.
SAMPLE_SPREADSHEET_ID = '1_dqw3Y3zH2zJhCiGARin963Sp5tejNoq1Z5MC8Z_W-I'
SAMPLE_RANGE_NAME = 'World of Warcraft!A1:L287' # Adjusted to your range

creds = None
if os.path.exists('token.pickle'):
    with open('token.pickle', 'rb') as token:
        creds = pickle.load(token)
if not creds or not creds.valid:
    if creds and creds.expired and creds.refresh_token:
        creds.refresh(Request())
    else:
        flow = InstalledAppFlow.from_client_secrets_file(
            'credentials.json', SCOPES)
        creds = flow.run_local_server(port=0)
    with open('token.pickle', 'wb') as token:
        pickle.dump(creds, token)

service = build('sheets', 'v4', credentials=creds)

# Call the Sheets API
sheet = service.spreadsheets()
result = sheet.values().get(spreadsheetId=SAMPLE_SPREADSHEET_ID,
                            range=SAMPLE_RANGE_NAME).execute()
values = result.get('values', [])

# Preprocess the data to ensure each row has exactly 12 elements
for i in range(len(values[2:])):
    length = len(values[2:][i])
    if length < 12:
        values[2:][i] += [None] * (12 - length)
    elif length > 12:
        values[2:][i] = values[2:][i][:12]

# Using pandas to convert list to JSON and then load it as a dictionary
print("Headers:", values[0])
print("First row:", values[1])
df = pd.DataFrame(values[2:], columns=values[1])
json_data = df.to_json(orient='records')
data = json.loads(json_data)

# Connecting to MongoDB
username = "kcparks1234"
password = "TTs2JytYedkzoOpg"
dbname = "gdata"
client = MongoClient(f"mongodb+srv://{username}:{password}@cluster0.eqfzrq7.mongodb.net/{dbname}?retryWrites=true&w=majority&tlsAllowInvalidCertificates=true")
db = client[dbname]

# Specifying the collection
collection = db['gdata']

# Inserting the data into the collection
collection.insert_many(data)
