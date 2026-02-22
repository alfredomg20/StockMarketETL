from datetime import datetime, date, timedelta
from google.cloud import bigquery
import pytz

def get_current_time_in_new_york() -> datetime:
    TIMEZONE = "America/New_York"
    tz = pytz.timezone(TIMEZONE)
    now = datetime.now(tz)
    return now

def adjust_to_business_day(date: date) -> date:
    if date.weekday() == 5:  # Saturday
        return date - timedelta(days=1)
    elif date.weekday() == 6:  # Sunday
        return date - timedelta(days=2)
    return date

def get_last_market_close_date() -> date:
    """ Returns the last market close date based on the current time in New York timezone."""
    now = get_current_time_in_new_york()
    today = now.date()

    if now.time() < datetime.strptime("16:00:00", "%H:%M:%S").time():
        reference_date = today - timedelta(days=1)
    else:
        reference_date = today

    return adjust_to_business_day(reference_date)
    