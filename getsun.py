# gets sunrise and sunset times
from datetime import date
from astral import sun
from astral import LocationInfo


def getsunrise(location, date):
    # get sunrise
    city = LocationInfo(location)
    sunrise = sun.sunrise(city.observer, date)
    return sunrise


def getsunset(location, date):
    # get sunset
    city = LocationInfo(location)
    sunset = sun.sunset(city.observer, date)
    return sunset
