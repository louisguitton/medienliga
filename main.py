from __future__ import print_function
import datetime
from datetime import datetime, timedelta
import pickle
import os.path
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from googleapiclient.errors import HttpError
import pandas as pd

# If modifying these scopes, delete the file token.pickle.
SCOPES = ["https://www.googleapis.com/auth/calendar"]


def parse_calendar() -> pd.DataFrame:
    df = (
        pd.read_excel(
            "data/Medienliga.xlsx",
            skiprows=1,
            usecols=["Spieltag", "Datum", "Uhrzeit", "Platz", "Partie", "Partie.1"],
        )
        .dropna()
        .assign(Spieltag=lambda df: df.Spieltag.astype(int))
        .assign(Platz=lambda df: df.Platz.astype(int))
        .assign(
            start_time=lambda df: df.apply(
                lambda row: row["Datum"]
                + timedelta(
                    hours=int(str(row["Uhrzeit"]).split(".")[0]),
                    minutes=int(str(row["Uhrzeit"]).split(".")[1]),
                ),
                axis=1,
            )
        )
        .assign(end_time=lambda df: df.start_time + timedelta(hours=1))
    )
    return df[["Spieltag", "Platz", "Partie.1", "start_time", "end_time"]]


def add_event(service, summary, description, start, end):
    event = {
        "summary": summary,
        "location": "Friedrich-Ludwig-Jahn-Sportpark, Cantianstraße 24, 10",
        "description": f"7 a side Medienliga football game\n{description}",
        "start": {
            "dateTime": start.isoformat(),
            "timeZone": "Europe/Berlin",
        },
        "end": {
            "dateTime": end.isoformat(),
            "timeZone": "Europe/Berlin",
        },
    }

    event = (
        service.events()
        .insert(
            calendarId="onefootball.com_dq5mdbp999kmli57krpego4f8c@group.calendar.google.com",
            body=event,
        )
        .execute()
    )
    return event


def main():
    """Shows basic usage of the Google Calendar API.
    Prints the start and name of the next 10 events on the user's calendar.
    """
    creds = None
    # The file token.json stores the user's access and refresh tokens, and is
    # created automatically when the authorization flow completes for the first
    # time.
    if os.path.exists("token.json"):
        creds = Credentials.from_authorized_user_file("token.json", SCOPES)
    # If there are no (valid) credentials available, let the user log in.
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file("credentials.json", SCOPES)
            creds = flow.run_local_server(port=0)
        # Save the credentials for the next run
        with open("token.json", "w") as token:
            token.write(creds.to_json())

    try:
        service = build("calendar", "v3", credentials=creds)
    
        # Call the Calendar API
        for spieltag, platz, partie, start, end in parse_calendar().itertuples(
            index=False, name=None
        ):
            e = add_event(
                service=service,
                summary=partie,
                description=f"Spieltag {spieltag}\nPlatz {platz}",
                start=start,
                end=end,
            )
            print(e.get("htmlLink"))

    except HttpError as error:
        print("An error occurred: %s" % error)


if __name__ == "__main__":
    main()
