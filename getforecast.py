import openmeteo_requests

import requests_cache
import pandas as pd
from retry_requests import retry
import matplotlib.pyplot as plt
import datetime
import argparse

from getstationdata import get_station_data

# import debugpy

# debugpy.listen(5678)
# debugpy.wait_for_client()


def generate_labels(dates):
    labels = []
    previous_date = None
    for date in dates:
        if date.date() != previous_date:
            labels.append(date.strftime("%Y-%m-%d %H:%M"))  # Show full date and time
            previous_date = date.date()
        else:
            labels.append(date.strftime("%H:%M"))  # Show only time
    return labels


def get_forecast(location, hours_to_show, past_count_of_15_minutes):
    # Setup the Open-Meteo API client with cache and retry on error
    cache_session = requests_cache.CachedSession(".cache", expire_after=3600)
    retry_session = retry(cache_session, retries=5, backoff_factor=0.2)
    openmeteo = openmeteo_requests.Client(session=retry_session)

    # parse the location
    # match anything starting with wack to a specific location
    if location.lower().startswith("wac") or location.lower().startswith("wak"):
        latitude = 54.75455
        longitude = 9.87333
    elif location.lower().startswith("fal"):
        latitude = 54.77019
        longitude = 9.965711
    else:
        raise ValueError("Location not supported")

    # Make sure all required weather variables are listed here
    url = "https://api.open-meteo.com/v1/forecast"
    params = {
        "latitude": latitude,
        "longitude": longitude,
        # "hourly": [
        #     "apparent_temperature",
        #     "precipitation",
        #     "wind_speed_10m",
        #     "wind_direction_10m",
        #     "wind_gusts_10m",
        # ],
        "minutely_15": [
            "apparent_temperature",
            "precipitation",
            "wind_speed_10m",
            "wind_direction_10m",
            "wind_gusts_10m",
        ],
        "wind_speed_unit": "kn",
        # "forecast_hours": 48,
        # "past_hours": 1,
        "past_minutely_15": past_count_of_15_minutes,
        "forecast_minutely_15": hours_to_show * 4,
        "timeformat": "unixtime",
        "timezone": "Europe/Berlin",
        # "models": ["arome_france_hd", "icon_d2", "metno_seamless"],
        "models": ["icon_d2"],
    }
    numbers_to_models = {
        11: "arome_france_hd",  # 1.5km, quarter hourly, every hour updated
        75: "metno_seamless",  # 1km, hourly, every hour updated
        23: "icon_d2",  # 2kn, hourly, every hour updated
    }
    responses = openmeteo.weather_api(url, params=params)

    # Process first location. Add a for-loop for multiple locations or weather models
    for response in responses:
        print(f"\nModel {numbers_to_models[response.Model()]}")
        print(f"Coordinates {response.Latitude()}°N {response.Longitude()}°E")
        # print(f"Elevation {response.Elevation()} m asl")
        # print(f"Current {response.Current()}")
        # print(f"Daily {response.Daily()}")

        hourly = response.Hourly()
        if hourly:
            hourly_apparent_temperature = hourly.Variables(0).ValuesAsNumpy()
            hourly_precipitation = hourly.Variables(1).ValuesAsNumpy()
            hourly_wind_speed_10m = hourly.Variables(2).ValuesAsNumpy()
            hourly_wind_direction_10m = hourly.Variables(3).ValuesAsNumpy()
            hourly_wind_gusts_10m = hourly.Variables(4).ValuesAsNumpy()

            hourly_data = {
                "date": pd.date_range(
                    start=pd.to_datetime(hourly.Time(), unit="s", utc=True),
                    end=pd.to_datetime(hourly.TimeEnd(), unit="s", utc=True),
                    freq=pd.Timedelta(seconds=hourly.Interval()),
                    inclusive="left",
                )
            }
            hourly_data["apparent_temperature"] = hourly_apparent_temperature
            hourly_data["precipitation"] = hourly_precipitation
            hourly_data["wind_speed_10m"] = hourly_wind_speed_10m
            hourly_data["wind_direction_10m"] = hourly_wind_direction_10m
            hourly_data["wind_gusts_10m"] = hourly_wind_gusts_10m

            hourly_dataframe = pd.DataFrame(data=hourly_data)
            print(hourly_dataframe.to_string())

        minutely_15 = response.Minutely15()
        if minutely_15:
            minutely_15_apparent_temperature = minutely_15.Variables(0).ValuesAsNumpy()
            minutely_15_precipitation = minutely_15.Variables(1).ValuesAsNumpy()
            minutely_15_wind_speed_10m = minutely_15.Variables(2).ValuesAsNumpy()
            minutely_15_wind_direction_10m = minutely_15.Variables(3).ValuesAsNumpy()
            minutely_15_wind_gusts_10m = minutely_15.Variables(4).ValuesAsNumpy()

            minutely_15_data = {
                "date": pd.date_range(
                    start=pd.to_datetime(minutely_15.Time(), unit="s", utc=True),
                    end=pd.to_datetime(minutely_15.TimeEnd(), unit="s", utc=True),
                    freq=pd.Timedelta(seconds=minutely_15.Interval()),
                    inclusive="left",
                )
            }
            minutely_15_data["wind_speed_10m"] = minutely_15_wind_speed_10m
            minutely_15_data["wind_gusts_10m"] = minutely_15_wind_gusts_10m
            minutely_15_data["wind_direction_10m"] = minutely_15_wind_direction_10m
            minutely_15_data["apparent_temperature"] = minutely_15_apparent_temperature
            minutely_15_data["precipitation"] = minutely_15_precipitation

            df = pd.DataFrame(data=minutely_15_data)
            # if we have wind_gusts_10m of 0 and wind_speed_10m of not 0, we need to set wind_gusts_10m to the previous value
            # print(df.to_string())
            # df_mask = df["wind_gusts_10m"] == 0
            # df["wind_gusts_10m"] = df["wind_gusts_10m"].mask(
            #     df_mask, df["wind_gusts_10m"].ffill()
            # )

            # we need to correct the date and time to the correct timezone
            df["date"] = df["date"] + pd.Timedelta(seconds=response.UtcOffsetSeconds())

            # convert the date to iso format
            df["date"] = df["date"].dt.strftime("%Y-%m-%dT%H:%M:%S")
            df["datetime"] = pd.to_datetime(df["date"])
            df["date"] = df["datetime"].dt.date
            df["time"] = df["datetime"].dt.time

            # print(df.to_string())
            df.to_csv(f"{numbers_to_models[response.Model()]}_minutely_15.csv")

            # get the station data
            now = datetime.datetime.now()
            yesterday = now - datetime.timedelta(days=1)
            from_time = now - datetime.timedelta(minutes=past_count_of_15_minutes * 15)
            station_data = get_station_data(from_time, now, sliding_window=15)
            # combine the two dataframes, we need to match the time, if the station data is missing, use the last value before the time being processed
            df.set_index("datetime", inplace=True)
            station_data.set_index("datetime", inplace=True)
            station_data_reindexed = station_data.reindex(df.index, method="backfill")
            df = df.join(station_data_reindexed["smooth_wind_avg"])
            df = df.join(station_data_reindexed["smooth_wind_min"])
            df = df.join(station_data_reindexed["smooth_wind_max"])
            print(df.to_string())
            df.reset_index(inplace=True)
            x_labels = generate_labels(df["datetime"][::4])
            tick_positions = df["datetime"][::4]
            tick_labels = x_labels
            # plot the wind speed and gusts in one plot
            plt.figure(figsize=(10, 5))
            plt.plot(
                df["datetime"],
                df["wind_speed_10m"],
                label="wind speed",
                linestyle="dashed",
                color="green",
            )
            plt.plot(
                df["datetime"],
                df["wind_gusts_10m"],
                label="wind gusts",
                linestyle="dashed",
                color="red",
            )
            plt.plot(
                df["datetime"],
                df["smooth_wind_max"],
                label="wind gusts",
                linestyle="solid",
                color="red",
            )
            plt.plot(
                df["datetime"],
                df["smooth_wind_min"],
                label="wind minimum",
                linestyle="solid",
                color="blue",
            )
            plt.plot(
                df["datetime"],
                df["smooth_wind_avg"],
                label="wind average",
                linestyle="solid",
                color="green",
            )
            # add gridlines
            plt.grid(True)
            plt.xticks(tick_positions, tick_labels, rotation=45, ha="right", va="top")
            plt.legend()
            plt.title(f"{numbers_to_models[response.Model()]} wind speed and gusts")
            plt.xlabel("Time")
            plt.ylabel("Wind Speed [kn]")
            # show the plot
            plt.show()


def parse_args():
    parser = argparse.ArgumentParser(description="Get forecast data from Open-Meteo")
    parser.add_argument(
        "-l",
        "--location",
        type=str,
        help="Location to get the forecast for",
        required=True,
    )
    parser.add_argument(
        "-t",
        "--hours_to_show",
        type=int,
        help="Number of hours to show in the forecast",
        required=False,
        default=72,
    )
    parser.add_argument(
        "-p",
        "--past_hours",
        type=int,
        help="Number of past hours to show in the forecast",
        required=False,
        default=18,
    )
    return parser.parse_args()


if __name__ == "__main__":

    # past_count_of_15_minutes = 100  # max is 8832

    args = parse_args()
    past_count_of_15_minutes = args.past_hours * 4
    get_forecast(args.location, args.hours_to_show, past_count_of_15_minutes)
