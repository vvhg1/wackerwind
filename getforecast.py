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
    previous_hour = None
    for date in dates:
        if date.date() != previous_date:
            labels.append(date.strftime("%H\n%Y-%m-%d"))  # Show full date and time
            previous_date = date.date()
        elif date.hour != previous_hour:
            labels.append(date.strftime("%H"))  # Show only time
            previous_hour = date.hour
    return labels


def get_forecast(location, hours_to_show, past_count_of_15_minutes):
    # Setup the Open-Meteo API client with cache and retry on error
    cache_session = requests_cache.CachedSession(".cache", expire_after=3600)
    retry_session = retry(cache_session, retries=5, backoff_factor=0.2)
    openmeteo = openmeteo_requests.Client(session=retry_session)

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
        "past_minutely_15": past_count_of_15_minutes,
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

    mse_df = pd.DataFrame()
    models_df = pd.DataFrame()
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
            station_data.set_index("datetime", inplace=False)
            station_data_reindexed = station_data.reindex(df.index, method=None)
            # where we have no measurements, get the closest measurement from station_data, but only if the measurement is not older than the previous datetime in station_data_reindexed
            new_station_data = pd.DataFrame(columns=station_data_reindexed.columns)

            for idx, row in station_data_reindexed.iterrows():
                if pd.isna(row["wind_avg"]):
                    this_datetime = row.name
                    # get the last datetime in station_data that is older than this datetime
                    station_data_row = (
                        station_data.loc[station_data["datetime"] < this_datetime]
                        .sort_values(by="datetime")
                        .tail(1)
                    )
                    if station_data_row.empty:
                        df.drop(idx, inplace=True)
                        station_data_reindexed.drop(idx, inplace=True)
                        continue
                    if this_datetime > now:
                        # add a nan to the dataframe
                        new_station_data.loc[idx, "smooth_wind_avg"] = pd.NA
                        new_station_data.loc[idx, "smooth_wind_min"] = pd.NA
                        new_station_data.loc[idx, "smooth_wind_max"] = pd.NA
                        continue
                    time_difference = (
                        this_datetime - station_data_row.iloc[0]["datetime"]
                    )
                    # if it is more than 15 minutes old
                    if time_difference > pd.Timedelta(minutes=15):
                        df.drop(idx, inplace=True)
                        continue
                    row = station_data_row.iloc[0]
                    # add the row to the dataframe
                    new_station_data.loc[idx] = row
            station_data_reindexed = new_station_data

            df = df.join(station_data_reindexed["smooth_wind_avg"])
            df = df.join(station_data_reindexed["smooth_wind_min"])
            df = df.join(station_data_reindexed["smooth_wind_max"])
            df.reset_index(inplace=True)
            print(df.to_string())
            # WARN: the icon_d2 model has faulty data for gusts in the past, so we need to correct it
            if numbers_to_models[response.Model()] == "icon_d2":
                # if we find a 0 wind speed, we need to correct the wind gusts, total 7 points, 3 before and 3 after
                for i in range(4, len(df["wind_speed_10m"]) - 4):
                    if df["wind_gusts_10m"][i] == 0:
                        df.loc[i, "wind_gusts_10m"] = (
                            df["wind_gusts_10m"][i - 4] + df["wind_gusts_10m"][i + 4]
                        ) / 2
                        df.loc[i + 2, "wind_gusts_10m"] = (
                            df["wind_gusts_10m"][i] + df["wind_gusts_10m"][i + 4]
                        ) / 2
                        df.loc[i - 2, "wind_gusts_10m"] = (
                            df["wind_gusts_10m"][i] + df["wind_gusts_10m"][i - 4]
                        ) / 2
                        df.loc[i - 1, "wind_gusts_10m"] = (
                            df["wind_gusts_10m"][i] + df["wind_gusts_10m"][i - 2]
                        ) / 2
                        df.loc[i + 1, "wind_gusts_10m"] = (
                            df["wind_gusts_10m"][i] + df["wind_gusts_10m"][i + 2]
                        ) / 2
                        df.loc[i + 3, "wind_gusts_10m"] = (
                            df["wind_gusts_10m"][i + 2] + df["wind_gusts_10m"][i + 4]
                        ) / 2
                        df.loc[i - 3, "wind_gusts_10m"] = (
                            df["wind_gusts_10m"][i - 2] + df["wind_gusts_10m"][i - 4]
                        ) / 2

            if mse_df.empty:
                mse_df["datetime"] = df["datetime"]
                mse_df["date"] = df["date"]
                mse_df["time"] = df["time"]
            mse_df["wind_speed_10m"] = df["wind_speed_10m"]
            mse_df["smooth_wind_avg"] = df["smooth_wind_avg"]
            mse_df["wind_gusts_10m"] = df["wind_gusts_10m"]
            mse_df["smooth_wind_max"] = df["smooth_wind_max"]

            # calculate the mean squared error
            mse_df = mse_df[mse_df["smooth_wind_avg"].notna()]
            mse_df[f"mse_wind_{numbers_to_models[response.Model()]}"] = (
                mse_df["wind_speed_10m"] - mse_df["smooth_wind_avg"]
            ) ** 2
            mse_df[f"mse_wind_gusts_{numbers_to_models[response.Model()]}"] = (
                mse_df["wind_gusts_10m"] - mse_df["smooth_wind_max"]
            ) ** 2
            mse_df[f"mse_{numbers_to_models[response.Model()]}"] = (
                mse_df[f"mse_wind_{numbers_to_models[response.Model()]}"]
                # + mse_df[f"mse_wind_gusts_{numbers_to_models[response.Model()]}"]
            )
            # smooth over the last 10 data points
            mse_df[f"mse_smooth_{numbers_to_models[response.Model()]}"] = (
                mse_df[f"mse_{numbers_to_models[response.Model()]}"]
                .rolling(10, min_periods=1)
                .mean()
            )

            models_df["datetime"] = df["datetime"]
            models_df[f"{numbers_to_models[response.Model()]}_wind_speed_10m"] = df[
                "wind_speed_10m"
            ]
            models_df[f"{numbers_to_models[response.Model()]}_wind_gusts_10m"] = df[
                "wind_gusts_10m"
            ]
            models_df["smooth_wind_max"] = df["smooth_wind_max"]
            models_df["smooth_wind_min"] = df["smooth_wind_min"]
            models_df["smooth_wind_avg"] = df["smooth_wind_avg"]
            models_df[f"{numbers_to_models[response.Model()]}_mse_smooth"] = mse_df[
                f"mse_smooth_{numbers_to_models[response.Model()]}"
            ]

            # x_labels = generate_labels(df["datetime"][::4])
            # tick_positions = df["datetime"][::4]
            # tick_labels = x_labels
            # # plot the wind speed and gusts in one plot
            # plt.figure(figsize=(30, 5))
            # plt.plot(
            #     df["datetime"],
            #     df["wind_speed_10m"],
            #     label="wind speed",
            #     linestyle="dashed",
            #     color="green",
            # )
            # plt.plot(
            #     df["datetime"],
            #     df["wind_gusts_10m"],
            #     label="wind gusts",
            #     linestyle="dashed",
            #     color="red",
            # )
            # plt.plot(
            #     df["datetime"],
            #     df["smooth_wind_max"],
            #     label="wind gusts",
            #     linestyle="solid",
            #     color="red",
            # )
            # plt.plot(
            #     df["datetime"],
            #     df["smooth_wind_min"],
            #     label="wind minimum",
            #     linestyle="solid",
            #     color="blue",
            # )
            # plt.plot(
            #     df["datetime"],
            #     df["smooth_wind_avg"],
            #     label="wind average",
            #     linestyle="solid",
            #     color="green",
            # )
            # plt.plot(
            #     mse_df["datetime"],
            #     mse_df[f"mse_smooth_{numbers_to_models[response.Model()]}"],
            #     label=f"MSE {numbers_to_models[response.Model()]}",
            #     linestyle="dotted",
            #     color="black",
            # )
            # # add gridlines
            # plt.grid(True)
            # plt.axhspan(15, 20, color="green", alpha=0.2)
            # plt.axhspan(20, 25, color="orange", alpha=0.2)
            # plt.axhspan(25, 30, color="red", alpha=0.2)
            # plt.xticks(tick_positions, tick_labels, fontsize=10)
            # plt.legend()
            # plt.title(f"{numbers_to_models[response.Model()]} wind speed and gusts")
            # plt.xlabel("Time")
            # plt.ylabel("Wind Speed [kn]")
            # # show the plot
            # plt.show()

    # print(mse_df.to_string())
    # total mse
    # cap the mse at 20
    for i, model in enumerate(models):
        total_mse = mse_df[f"mse_{model}"].clip(upper=20).sum()
        print(f"Total MSE for {model}: {total_mse}")
    # plot the mean squared error
    print(models_df.to_string())
    plt.figure(figsize=(30, 10))
    colors = ["red", "green", "blue"]
    # colors = ["lightskyblue", "limegreen", "orange"]
    x_labels = generate_labels(models_df["datetime"][::4])
    tick_positions = models_df["datetime"][::4]
    tick_labels = x_labels
    for i, model in enumerate(models):
        plt.plot(
            models_df["datetime"],
            models_df[f"{model}_wind_speed_10m"],
            label=f"{model}",
            linestyle="solid",
            color=colors[i],
        )
        plt.plot(
            models_df["datetime"],
            models_df[f"{model}_wind_gusts_10m"],
            linestyle="dashed",
            color=colors[i],
        )
        # plt.plot(
        #     models_df["datetime"],
        #     models_df[f"{model}_mse_smooth"],
        #     linestyle="dotted",
        #     color=colors[i],
        # )
    plt.plot(
        models_df["datetime"],
        models_df["smooth_wind_avg"],
        label="Avg",
        linestyle="solid",
        color="gray",
    )
    plt.plot(
        models_df["datetime"],
        models_df["smooth_wind_min"],
        label="Min",
        linestyle="dotted",
        color="gray",
    )
    plt.plot(
        models_df["datetime"],
        models_df["smooth_wind_max"],
        label="Max",
        linestyle="dashed",
        color="gray",
    )
    # Add plot details
    plt.grid(True)
    plt.axhspan(15, 20, color="green", alpha=0.2)
    plt.axhspan(20, 25, color="orange", alpha=0.2)
    plt.axhspan(25, 30, color="red", alpha=0.2)
    plt.xticks(tick_positions, tick_labels, fontsize=8)
    plt.legend()
    plt.title(f"Wind forecast for {args.location}")
    plt.xlabel("Time")
    plt.ylabel("Wind Speed [kn]")
    plt.show()

    # plot the mean squared error
    print(models_df.to_string())
    plt.figure(figsize=(30, 10))
    colors = ["red", "green", "blue"]
    x_labels = generate_labels(models_df["datetime"][::4])
    tick_positions = models_df["datetime"][::4]
    tick_labels = x_labels
    for i, model in enumerate(models):
        # plt.plot(
        #     models_df["datetime"],
        #     models_df[f"{model}_wind_speed_10m"],
        #     label=f"{model}",
        #     linestyle="solid",
        #     color=colors[i],
        # )
        # plt.plot(
        #     models_df["datetime"],
        #     models_df[f"{model}_wind_gusts_10m"],
        #     linestyle="dashed",
        #     color=colors[i],
        # )
        plt.plot(
            models_df["datetime"],
            models_df[f"{model}_mse_smooth"],
            label=f"MSE {model}",
            linestyle="dotted",
            color=colors[i],
        )
    plt.grid(True)
    plt.xticks(tick_positions, tick_labels, fontsize=8)
    plt.legend()
    plt.title(f"MSE for {args.location}")
    plt.xlabel("Time")
    plt.ylabel("MSE")
    plt.show()


def parse_args():
    def intrange(min, max):
        def check(value):
            ivalue = int(value)
            if min <= ivalue <= max:
                return ivalue
            else:
                raise argparse.ArgumentTypeError(f"{value} is not in range {min}-{max}")

        return check

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
        type=intrange(0, 2208),
        help="Number of past hours to show in the forecast, must be between 0 and 2208",
        required=False,
        default=18,
    )
    return parser.parse_args()


if __name__ == "__main__":

    # past_count_of_15_minutes = 100  # max is 8832

    args = parse_args()
    print(args.past_hours)
    past_count_of_15_minutes = args.past_hours * 4
    get_forecast(args.location, args.hours_to_show, past_count_of_15_minutes)
