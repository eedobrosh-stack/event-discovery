"""Google Sheets export — requires OAuth setup.

To use:
1. Set GOOGLE_CLIENT_ID and GOOGLE_CLIENT_SECRET in .env
2. Complete OAuth flow via /api/auth/google
3. Call POST /api/export/sheets with filter params
"""


def export_to_sheets(events, title: str = "Event Export", credentials=None) -> str:
    """Export events to a new Google Sheet. Returns the spreadsheet URL."""
    if not credentials:
        raise ValueError("Google OAuth credentials required. Complete /api/auth/google first.")

    from googleapiclient.discovery import build

    service = build("sheets", "v4", credentials=credentials)

    # Create spreadsheet
    spreadsheet = service.spreadsheets().create(
        body={
            "properties": {"title": title},
            "sheets": [{"properties": {"title": "Events"}}],
        }
    ).execute()

    spreadsheet_id = spreadsheet["spreadsheetId"]

    # Build rows
    headers = [
        "Event Name", "Artist", "Start Date", "Start Time",
        "End Date", "End Time", "Venue", "City", "Price",
        "Currency", "Purchase Link", "Category", "Source",
    ]

    rows = [headers]
    for e in events:
        categories = ", ".join(et.category for et in e.event_types if et.category)
        rows.append([
            e.name,
            e.artist_name or "",
            str(e.start_date) if e.start_date else "",
            e.start_time or "",
            str(e.end_date) if e.end_date else "",
            e.end_time or "",
            e.venue_name or "",
            e.venue.physical_city if e.venue else "",
            str(e.price) if e.price else "",
            e.price_currency or "",
            e.purchase_link or "",
            categories,
            e.scrape_source or "",
        ])

    service.spreadsheets().values().update(
        spreadsheetId=spreadsheet_id,
        range="Events!A1",
        valueInputOption="RAW",
        body={"values": rows},
    ).execute()

    return f"https://docs.google.com/spreadsheets/d/{spreadsheet_id}"
