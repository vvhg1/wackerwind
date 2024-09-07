import openmeteo_requests

import requests_cache
import pandas as pd
from retry_requests import retry
import matplotlib.pyplot as plt
import datetime
import os


def save_forecast(location, hours_to_show):
    # Setup the Open-Meteo API client with cache and retry on error
    cache_session = requests_cache.CachedSession(".cache", expire_after=3600)
    retry_session = retry(cache_session, retries=5, backoff_factor=0.2)
    openmeteo = openmeteo_requests.Client(session=retry_session)
    save_path = "/home/vhg/repos/wackerwind/data/forecasts/"

    models = ["arome_france_hd", "icon_d2", "metno_seamless"]
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
        "past_minutely_15": 0,
        "forecast_minutely_15": hours_to_show * 4,
        "timeformat": "unixtime",
        "timezone": "Europe/Berlin",
        "models": models,
        # "models": ["icon_d2"],
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

            # drop anything that is in the past
            now = datetime.datetime.now()
            df = df[df["datetime"] > now - pd.Timedelta(minutes=2)]
            print(df)

            # we drop anything where the wind_speed_10m is nan
            df = df[df["wind_speed_10m"].notna()]

            # we split the data according to how far it looks into the future, we start at the back as we know how many hours we got in the forecast, that way we know how old they are
            last_forecast_time = df["datetime"].max()
            countdown = hours_to_show
            # we check if we have new data, for that we read the last forecast time from the hdf5 file
            if os.path.exists(
                f"{save_path}{location}_{numbers_to_models[response.Model()]}_{countdown}.h5"
            ):
                while True:
                    prev_last_forecast_time = ""
                    with pd.HDFStore(
                        f"{save_path}{location}_{numbers_to_models[response.Model()]}_{countdown}.h5"
                    ) as store:
                        temp_df = store["data"]
                        # turn the datetime into a datetime, we have to amend this as datetime is now the index
                        temp_df["datetime"] = pd.to_datetime(temp_df.index)
                        # we check if the last forecast time is the same as the previous last forecast time
                        prev_last_forecast_time = temp_df["datetime"].max()
                        if last_forecast_time == prev_last_forecast_time:
                            print("No new data")
                            break
                        # we get every forecast that is newer than the last forecast time
                        this_hour_df = df[
                            df["datetime"] > prev_last_forecast_time
                        ].copy()
                        this_hour_df["save_time"] = now
                        this_hour_df["save_time"] = this_hour_df[
                            "save_time"
                        ].dt.strftime("%Y-%m-%dT%H:%M:%S")
                        # turn the datetime into a string
                        this_hour_df["datetime"] = this_hour_df["datetime"].dt.strftime(
                            "%Y-%m-%dT%H:%M:%S"
                        )
                        # we want the datetime to be the index
                        this_hour_df = this_hour_df.set_index("datetime")
                        print(this_hour_df.to_string())
                        # we save the current hourly data to hdf5
                        with pd.HDFStore(
                            f"{save_path}{location}_{numbers_to_models[response.Model()]}_{countdown}.h5"
                        ) as store:
                            store.append("data", this_hour_df, format="table")
                        # we drop the ones that we have already saved
                        df = df.drop(df[df["datetime"] > prev_last_forecast_time].index)
                        # we decrease the countdown
                        countdown -= 1
                        if countdown == 0:
                            break
            else:
                while df.empty == False:
                    print("first run")
                    # we get the last four values as that is the hours_to_show's hour in the future
                    this_hour_df = df.tail(4).copy()
                    this_hour_df["save_time"] = now
                    this_hour_df["save_time"] = this_hour_df["save_time"].dt.strftime(
                        "%Y-%m-%dT%H:%M:%S"
                    )
                    # turn the datetime into a string
                    this_hour_df["datetime"] = this_hour_df["datetime"].dt.strftime(
                        "%Y-%m-%dT%H:%M:%S"
                    )
                    this_hour_df = this_hour_df.set_index("datetime")
                    print(this_hour_df.to_string())
                    # we save the current hourly data to hdf5
                    with pd.HDFStore(
                        f"{save_path}{location}_{numbers_to_models[response.Model()]}_{countdown}.h5"
                    ) as store:
                        store.append("data", this_hour_df, format="table")
                    # we drop the last four values
                    df = df.drop(df.tail(4).index)
                    # we decrease the countdown
                    countdown -= 1
                    if countdown == 0:
                        break


if __name__ == "__main__":
    # we call the function
    save_forecast("wac", 36)
    save_forecast("fal", 36)
