# get the data from the API

import requests
import pandas as pd
import json
import datetime
import matplotlib.pyplot as plt
import typing
import numpy as np


def generate_labels(dates: typing.List[datetime.datetime]) -> typing.List[str]:
    labels = []
    previous_date = None
    for date in dates:
        if date.date() != previous_date:
            labels.append(date.strftime("%Y-%m-%d %H:%M"))  # Show full date and time
            previous_date = date.date()
        else:
            labels.append(date.strftime("%H:%M"))  # Show only time
    return labels


def get_station_data(
    station: str,
    from_date: datetime.datetime,
    to_date: datetime.datetime,
    sliding_window: int = 1,
) -> pd.DataFrame:
    if station == "wak":
        return get_station_data_wak(from_date, to_date, sliding_window)
    elif station == "keg":
        return get_station_data_meteostat("06119", from_date, to_date, sliding_window)
    elif station == "olp":
        return get_station_data_meteostat("10042", from_date, to_date, sliding_window)
    elif station == "lis":
        return get_station_data_meteostat(
            "10020", from_date, to_date, sliding_window
        )  # List/Sylt
    else:
        # empty dataframe
        return pd.DataFrame()


def get_station_data_meteostat(
    station: str,
    from_date: datetime.datetime,
    to_date: datetime.datetime,
    sliding_window: int = 1,
) -> pd.DataFrame:
    print(f"Getting station data for keg")
    url = "https://d.meteostat.net/app/proxy/stations/hourly"
    params = {
        "station": station,
        "tz": "Europe/Copenhagen",
        "start": f"{from_date.strftime('%Y-%m-%d')}",
        "end": f"{to_date.strftime('%Y-%m-%d')}",
    }
    # Updated headers
    headers = {
        "authority": "d.meteostat.net",
        "accept": "*/*",
        "accept-language": "en-US,en;q=0.9",
        "dnt": "1",
        "origin": "https://meteostat.net",
        "referer": "https://meteostat.net/",
        "sec-fetch-dest": "empty",
        "sec-fetch-mode": "cors",
        "sec-fetch-site": "same-site",
        "user-agent": "Mozilla/5.0 (Windows NT 10.0; rv:102.0) Gecko/20100101 Firefox/102.0",
        "connection": "keep-alive",
    }
    session = requests.Session()
    session.headers.update(headers)
    response = session.get(url, params=params)
    station_data = response.json()
    df = pd.DataFrame(station_data["data"])
    df = df.rename(
        columns={
            "wspd": "wind_avg",
            "wpgt": "wind_max",
            "wdir": "wind_dir",
            "temp": "temperature",
        }
    )
    # convert from km/h to kn
    df["wind_avg"] = df["wind_avg"] / 1.852
    df["wind_max"] = df["wind_max"] / 1.852
    # fill min with zeros as we don't have it
    df["wind_min"] = 0
    df["datetime"] = pd.to_datetime(df["time"], utc=True)
    df["date"] = df["datetime"].dt.date
    df["time"] = df["datetime"].dt.time
    df["smooth_wind_avg"] = df["wind_avg"]
    df["smooth_wind_max"] = df["wind_max"]
    df["smooth_wind_min"] = df["wind_min"]
    return df


def get_station_data_wak(
    from_date: datetime.datetime, to_date: datetime.datetime, sliding_window: int = 1
) -> pd.DataFrame:

    print(f"Getting station data for wak")
    url = "https://www.windguru.cz/int/iapi.php"
    params = {
        "q": "station_data",
        "id_station": "3737",
        # "from": f"{one_year_ago}T05:00:20.000Z",
        "from": f"{from_date.strftime('%Y-%m-%dT%H:%M:%S')}",
        "to": f"{to_date.strftime('%Y-%m-%dT%H:%M:%S')}",
        "avg_minutes": "1",
        "graph_info": "1",
    }

    # Updated headers
    headers = {
        "sec-ch-ua": '"Not=A?Brand";v="99", "Chromium";v="118"',
        "Referer": "https://www.windguru.cz/station/3737",
        "sec-ch-ua-mobile": "?0",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; rv:102.0) Gecko/20100101 Firefox/102.0",
        "sec-ch-ua-platform": '"Linux"',
        "Accept-Encoding": "gzip, deflate, br",
        "Accept-Language": "en-US,en;q=0.9",
        "Connection": "keep-alive",
    }

    # Using a session to better replicate curl behavior
    session = requests.Session()
    session.headers.update(headers)

    # Making the request
    response = session.get(url, params=params)
    # print(response.text)

    # Extract the data from the JSON response
    station_data = response.json()
    # print(station_data)

    # Create a dataframe from the data
    df = pd.DataFrame(station_data)
    # wiind_avg and wind_min are mixed up
    df = df.rename(columns={"wind_avg": "wind_min", "wind_min": "wind_avg"})
    # print(df.to_string())
    df["datetime"] = pd.to_datetime(df["datetime"])
    df["date"] = df["datetime"].dt.date
    df["time"] = df["datetime"].dt.time
    df["smooth_wind_avg"] = df["wind_avg"].rolling(window=sliding_window).mean()
    df["smooth_wind_min"] = df["wind_min"].rolling(window=sliding_window).min()
    df["smooth_wind_max"] = df["wind_max"].rolling(window=sliding_window).max()
    # print(df.to_string())
    return df


def main():
    now = datetime.datetime.now()
    hours_to_show = 24
    from_time = now - datetime.timedelta(hours=hours_to_show)
    df = get_station_data("keg", from_time, now)

    # print(df[10:].to_string())

    # here we show the last 20 hours, not the first 20 hours
    x_labels = generate_labels(df["datetime"][-hours_to_show * 60 :])
    tick_positions = df["datetime"][-hours_to_show * 60 :: 20]
    tick_labels = x_labels[::20]
    # plot the wind speed and gusts in one plot
    plt.figure(figsize=(10, 5))
    plt.plot(
        df["datetime"][-hours_to_show * 60 :],
        df["smooth_wind_min"][-hours_to_show * 60 :],
        label="wind minimum",
        linestyle="dotted",
        color="blue",
    )
    plt.plot(
        df["datetime"][-hours_to_show * 60 :],
        df["smooth_wind_avg"][-hours_to_show * 60 :],
        label="wind average",
        linestyle="solid",
        color="green",
    )
    plt.plot(
        df["datetime"][-hours_to_show * 60 :],
        df["smooth_wind_max"][-hours_to_show * 60 :],
        label="wind gusts",
        linestyle="dotted",
        color="red",
    )
    plt.plot(
        df["datetime"][-hours_to_show * 60 :],
        df["temperature"][-hours_to_show * 60 :],
        label="temperature",
        linestyle="solid",
        color="blue",
    )
    # plt.plot(
    #     df["datetime"][-hours_to_show * 60 :],
    #     df["wind_max"][-hours_to_show * 60 :],
    #     label="wind gusts",
    #     linestyle="solid",
    # )
    # plt.plot(
    #     df["datetime"][-hours_to_show * 60 :],
    #     df["wind_min"][-hours_to_show * 60 :],
    #     label="wind minimum",
    #     linestyle="dotted",
    # )
    # plt.plot(
    #     df["datetime"][-hours_to_show * 60 :],
    #     df["wind_avg"][-hours_to_show * 60 :],
    #     label="wind average",
    #     linestyle="dashed",
    # )
    # add gridlines
    plt.grid(True)
    plt.xticks(tick_positions, tick_labels, rotation=45, ha="right", va="top")
    plt.legend()
    plt.title("Station Wind Data")
    plt.xlabel("Time")
    plt.ylabel("Wind Speed [kn]")
    plt.show()


if __name__ == "__main__":
    main()
