#!/usr/bin/env python3
"""Re-authorize personal Google OAuth for HappyPet sheets/drive/apps-script access.
Run: python3 /home/derek/utils/reauth_google.py
"""
from google_auth_oauthlib.flow import InstalledAppFlow
import json, os

SCOPES = [
    'https://www.googleapis.com/auth/spreadsheets',
    'https://www.googleapis.com/auth/drive',
    'https://www.googleapis.com/auth/script.projects',
    'https://www.googleapis.com/auth/script.external_request',
]

# Load client credentials from existing token
TOKEN_FILE = os.path.expanduser("~/.happypet_token.json")
with open(TOKEN_FILE) as f:
    t = json.load(f)

CLIENT_CONFIG = {"installed": {
    "client_id":      t["client_id"],
    "client_secret":  t["client_secret"],
    "redirect_uris":  ["urn:ietf:wg:oauth:2.0:oob", "http://localhost"],
    "auth_uri":       "https://accounts.google.com/o/oauth2/auth",
    "token_uri":      "https://oauth2.googleapis.com/token"
}}

flow = InstalledAppFlow.from_client_config(CLIENT_CONFIG, SCOPES)
creds = flow.run_local_server(port=8080, open_browser=True)

token_data = {
    "token":         creds.token,
    "refresh_token": creds.refresh_token,
    "token_uri":     creds.token_uri,
    "client_id":     creds.client_id,
    "client_secret": creds.client_secret,
    "scopes":        list(creds.scopes)
}

with open(TOKEN_FILE, "w") as f:
    json.dump(token_data, f, indent=2)

print(f"✅ Token saved to {TOKEN_FILE}")
print(f"   Scopes: {list(creds.scopes)}")
