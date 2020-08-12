from datetime import datetime as pdatetime
from datetime import timezone as ptimezone

import common_settings as settings


def now():
    """
    Return the current time with configured timezone
    """
    return pdatetime.now(tz=settings.TZ)

def datetime(year,month=1,day=1,hour=0,minute=0,second=0,microsecond=0):
    return pdatetime(year,month,day,hour,minute,second,microsecond,tzinfo=settings.TZ)

def nativetime(d=None):
    """
    Return the datetime with configured timezone, 
    if d is None, return current time with configured timezone
    """
    if d:
        if d.tzinfo:
            return d.astimezone(settings.TZ)
        else:
            return d.replace(tzinfo=settings.TZ)
    else:
        return now()

def utctime(d=None):
    """
    Return the datetime with utc timezone, 
    if d is None, return current time with configured timezone
    """
    if not d:
        d  = now()

    return d.astimezone(ptimezone.utc)


def in_working_hour():
    hour = now().hour
    if settings.END_WORKING_HOUR is not None and hour <= settings.END_WORKING_HOUR:
        if settings.START_WORKING_HOUR is None or hour >= settings.START_WORKING_HOUR:
            return True

    if settings.START_WORKING_HOUR is not None and hour >= settings.START_WORKING_HOUR:
        if settings.END_WORKING_HOUR is None or hour <= settings.END_WORKING_HOUR:
            return True

    return False

