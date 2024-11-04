import requests
import json
import sys


def get_waterlevel(station):
    url = f"https://www.pegelonline.wsv.de/webservices/rest-api/v2/stations/{station}/W/measurements.json?start=P10D"
    r = requests.get(url)
    data = json.loads(r.text)
    return data


if __name__ == "__main__":
    station = "b09f2243-60f0-469a-8f3b-0ea6abc83267"  # kappeln
    station = "22b7dcb3-8c42-4f71-9191-49143ba3a828"  # kalkgrund
    print(get_waterlevel(station))
