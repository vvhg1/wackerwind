# get the data from the API

import requests
import pandas as pd
import json
import datetime
import matplotlib.pyplot as plt
import typing


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
    from_date: datetime.datetime, to_date: datetime.datetime, sliding_window: int = 1
) -> pd.DataFrame:

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

    # Create a dataframe from the data
    df = pd.DataFrame(station_data)
    # wiind_avg and wind_min are mixed up
    df = df.rename(columns={"wind_avg": "wind_min", "wind_min": "wind_avg"})
    df["datetime"] = pd.to_datetime(df["datetime"])
    df["date"] = df["datetime"].dt.date
    df["time"] = df["datetime"].dt.time
    df["smooth_wind_avg"] = df["wind_avg"].rolling(window=sliding_window).mean()
    df["smooth_wind_min"] = df["wind_min"].rolling(window=sliding_window).min()
    df["smooth_wind_max"] = df["wind_max"].rolling(window=sliding_window).max()
    return df


def main():
    now = datetime.datetime.now()
    one_year_ago = now - datetime.timedelta(days=365)
    yesterday = now - datetime.timedelta(days=1)
    hours_to_show = 10
    df = get_station_data(yesterday, now)

    print(df[10:].to_string())
    # df["datetime"] = df["datetime"] + pd.Timedelta(seconds=tz_offset)
    # df["datetime"] = pd.to_datetime(df["datetime"].dt.strftime("%Y-%m-%dT%H:%M:%S"))
    # df["datetime"] = pd.to_datetime(df["date"])
    # save as csv
    # df.to_csv("windguru.csv")

    # Print the dataframe
    # print(df.to_string())

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
    # show the plot
    plt.show()


if __name__ == "__main__":
    main()
