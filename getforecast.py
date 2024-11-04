import openmeteo_requests
import pandas

import requests_cache
import pandas as pd
import numpy as np
from retry_requests import retry
import matplotlib.pyplot as plt
import datetime
import argparse

from getstationdata import get_station_data
from getwaterlevel import get_waterlevel
from getsun import getsunrise, getsunset

# import debugpy

# debugpy.listen(5678)
# debugpy.wait_for_client()


def generate_labels(dates):
    labels = []
    tick_positions = []
    previous_date = None
    previous_hour = None
    for date in dates:
        if date.date() != previous_date:
            labels.append(date.strftime("%H\n%Y-%m-%d"))  # Show full date and time
            tick_positions.append(date)
            previous_date = date.date()
        elif date.hour != previous_hour:
            labels.append(date.strftime("%H"))  # Show only time
            tick_positions.append(date)
            previous_hour = date.hour
    return tick_positions, labels


def get_forecast(location, weatherstation, hours_to_show, past_count_of_15_minutes):
    # Setup the Open-Meteo API client with cache and retry on error
    print(f"Getting forecast for {location}")
    cache_session = requests_cache.CachedSession(".cache", expire_after=3600)
    retry_session = retry(cache_session, retries=5, backoff_factor=0.2)
    openmeteo = openmeteo_requests.Client(session=retry_session)

    models = [
        "arome_france_hd",
        "icon_d2",
        "metno_seamless",
        "dmi_harmonie_arome_europe",
        # "knmi_harmonie_arome_netherlands",
        # "ukmo_uk_deterministic_2km",
    ]
    # models = ["arome_france_hd", "icon_d2", "metno_seamless"]
    # parse the location
    # match anything starting with wack to a specific location
    if location.lower().startswith("wac") or location.lower().startswith("wak"):
        latitude = 54.75455
        longitude = 9.87333
        waterlevel = "22b7dcb3-8c42-4f71-9191-49143ba3a828"  # kalkgrund
    elif location.lower().startswith("fal"):
        latitude = 54.77019
        longitude = 9.965711
        waterlevel = "22b7dcb3-8c42-4f71-9191-49143ba3a828"  # kalkgrund
    elif location.lower().startswith("ohr"):
        latitude = 54.760344
        longitude = 9.837195
        waterlevel = "22b7dcb3-8c42-4f71-9191-49143ba3a828"  # kalkgrund
    elif location.lower().startswith("maas"):
        latitude = 54.683032
        longitude = 10.001216
        waterlevel = "b09f2243-60f0-469a-8f3b-0ea6abc83267"  # kappeln
    elif location.lower().startswith("rom"):
        latitude = 55.154645
        longitude = 8.474347
        waterlevel = "5e92d73f-e4ea-42c1-9f98-91536c17cdff"  # Römö
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
        74: "dmi_harmonie_arome_europe",  # 2km, hourly, every 3 hours updated
        72: "knmi_harmonie_arome_netherlands",  # 2km, hourly, every hour updated
        81: "ukmo_uk_deterministic_2km",  # 2km, hourly, every hour updated
    }
    responses = openmeteo.weather_api(url, params=params)

    mse_df = pd.DataFrame()
    models_df = pd.DataFrame()
    now = datetime.datetime.now()
    # yesterday = now - datetime.timedelta(days=1)
    from_time = now - datetime.timedelta(minutes=past_count_of_15_minutes * 15)
    station_data = get_station_data(weatherstation, from_time, now, sliding_window=15)
    print("got station data")
    # print(station_data)
    waterlevels = get_waterlevel(waterlevel)
    # convert the waterlevels to a dataframe
    waterlevels_df = pd.DataFrame(waterlevels)
    # rename timestamp to datetime
    waterlevels_df["roundedtimestamp"] = pd.to_datetime(
        waterlevels_df["timestamp"], utc=True
    ).dt.ceil("15min")
    waterlevels_df["roundedtimestamp"] = waterlevels_df["roundedtimestamp"].dt.strftime(
        "%Y-%m-%dT%H:%M:%S"
    )
    waterlevels_df["datetime"] = pd.to_datetime(waterlevels_df["roundedtimestamp"])
    waterlevels_df = waterlevels_df.drop_duplicates(subset=["datetime"], keep="last")
    # make unique, keep the last occurence
    waterlevels_df.set_index("datetime", inplace=True)
    # we need to round the index UP to the next 15 minutes
    # combine the two dataframes, we need to match the time, if the station data is missing, use the last value before the time being processed
    # correct station_data datetime to be in the same timezone as df
    station_data["datetime"] = pd.to_datetime(station_data["datetime"])
    station_data.set_index("datetime", inplace=False)
    # Process first location. Add a for-loop for multiple locations or weather models
    for response in responses:
        print(f"\nModel {numbers_to_models[response.Model()]}")
        # print(f"Coordinates {response.Latitude()}°N {response.Longitude()}°E")
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
            timezone = df["datetime"].dt.tz
            df["date"] = df["datetime"].dt.date
            df["time"] = df["datetime"].dt.time

            # print(df.to_string())
            # df.to_csv(f"{numbers_to_models[response.Model()]}_minutely_15.csv")

            # get the station data
            # TODO: better to get the station data indexed to quater hourly already
            df.set_index("datetime", inplace=True)
            station_data["datetime"] = station_data["datetime"].dt.tz_localize(timezone)
            station_data_reindexed = station_data.reindex(df.index, method=None)
            pandas.set_option("future.no_silent_downcasting", True)
            station_data_reindexed.replace(pd.NA, np.nan, inplace=True)
            waterlevels_reindexed = waterlevels_df.reindex(df.index, method=None)
            waterlevels_reindexed = waterlevels_reindexed.rename(
                columns={"value": "waterlevel"}
            )
            # waterlevel substract 500 to get the height in cm
            waterlevels_reindexed["waterlevel"] = (
                waterlevels_reindexed["waterlevel"] - 500
            )
            waterlevels_reindexed["waterlevel"] = (
                waterlevels_reindexed["waterlevel"] / 10
            )
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
                        new_station_data.loc[idx, "smooth_wind_avg"] = np.nan
                        new_station_data.loc[idx, "smooth_wind_min"] = np.nan
                        new_station_data.loc[idx, "smooth_wind_max"] = np.nan
                        continue
                    time_difference = (
                        this_datetime - station_data_row.iloc[0]["datetime"]
                    )
                    # if it is more than 15 minutes old
                    # if time_difference > pd.Timedelta(minutes=15):
                    #     new_station_data.loc[idx, "smooth_wind_avg"] = np.nan
                    #     new_station_data.loc[idx, "smooth_wind_min"] = np.nan
                    #     new_station_data.loc[idx, "smooth_wind_max"] = np.nan
                    #     # print(f"Dropping {idx}")
                    #     # df.drop(idx, inplace=True)
                    #     continue
                    row = station_data_row.iloc[0]
                    # add the row to the dataframe
                    new_station_data.loc[idx] = row
            station_data_reindexed = new_station_data

            df = df.join(station_data_reindexed["smooth_wind_avg"])
            df = df.join(station_data_reindexed["smooth_wind_min"])
            df = df.join(station_data_reindexed["smooth_wind_max"])
            df = df.join(waterlevels_reindexed["waterlevel"])
            print("data joined")
            print(df)
            df.reset_index(inplace=True)
            print("data reset")
            print(df)
            # print(df.to_string())
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

            if not models_df.empty:
                # check that we have the same number of rows
                if len(models_df) > len(df):
                    # check the first datetime
                    if models_df["datetime"].iloc[0] != df["datetime"].iloc[0]:
                        # if the first datetime is different, we need to prepend an empty row to df
                        na_row = pd.DataFrame(
                            [[pd.NA] * len(df.columns)], columns=df.columns
                        )
                        na_row["datetime"] = models_df["datetime"].iloc[0]
                        df = pd.concat([na_row, df])

            models_df["datetime"] = df["datetime"]
            models_df["waterlevel"] = df["waterlevel"]
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

            models_df[f"{numbers_to_models[response.Model()]}_wind_dir_U"] = [
                -np.sin(np.deg2rad(x)) for x in df["wind_direction_10m"]
            ]
            models_df[f"{numbers_to_models[response.Model()]}_wind_dir_V"] = [
                -np.cos(np.deg2rad(x)) for x in df["wind_direction_10m"]
            ]
    # cap the mse at 20
    for i, model in enumerate(models):
        total_mse = mse_df[f"mse_{model}"].clip(upper=20).sum()
        print(f"Total MSE for {model}: {total_mse}")
    # plot the mean squared error
    # print(models_df.to_string())
    # get sunrise and sunset times
    sunrise = getsunrise("Flensburg", models_df["datetime"].iloc[0]).replace(
        tzinfo=None
    )
    sunset = getsunset("Flensburg", models_df["datetime"].iloc[0]).replace(tzinfo=None)
    sunrise_time_of_day = sunrise.time()
    sunset_time_of_day = sunset.time()
    sunset = getsunset("Flensburg", models_df["datetime"].iloc[0]).replace(tzinfo=None)
    print(f"Sunrise: {sunrise}")
    print(f"Sunset: {sunset}")
    print("models_df")
    print(models_df)
    # we drop the first rows if they are not on the full hour
    while models_df["datetime"].iloc[0].minute != 0:
        # drop the first row
        models_df.drop(models_df.index[0], inplace=True)
    print("dropped first rows")
    print(models_df)
    plt.figure(figsize=(30, 10))
    colors = [
        "red",
        "green",
        "blue",
        "orange",
        "purple",
        "teal",
        "pink",
        "brown",
        "darkgreen",
    ]
    # add a column with a boolean value, if the time is between sunrise and sunset, the value is True
    models_df["is_night"] = models_df["datetime"].apply(
        lambda x: (
            True
            if x.time() < sunrise_time_of_day or x.time() > sunset_time_of_day
            else False
        )
    )
    # print(models_df)
    # shade the night
    plt.fill_between(
        models_df["datetime"],
        plt.ylim()[0],
        models_df["smooth_wind_max"].max(),
        where=models_df["is_night"],
        color="gray",
        alpha=0.2,
    )

    # colors = ["lightskyblue", "limegreen", "orange"]
    tick_positions, tick_labels = generate_labels(models_df["datetime"][::4])
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
        plt.quiver(
            models_df["datetime"][::4],
            np.zeros(len(models_df["datetime"][::4])),
            models_df[f"{model}_wind_dir_U"][::4],
            models_df[f"{model}_wind_dir_V"][::4],
            units="width",
            width=0.0015,
            pivot="mid",
            scale=70,
            headlength=5,
            headwidth=5,
            color=colors[i],
        )
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
    # plot the waterlevel on a separate scale
    plt.plot(
        models_df["datetime"],
        models_df["waterlevel"],
        label="Waterlevel",
        linestyle="solid",
        color="blue",
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
    # save the figure
    plt.savefig(f"../../Downloads/{args.location}.png")
    plt.show()

    # plot the mean squared error
    # print(models_df.to_string())
    plt.figure(figsize=(30, 10))
    # x_labels = generate_labels(models_df["datetime"])
    # tick_positions = models_df["datetime"][::4]
    # tick_labels = x_labels
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
        "-s",
        "--weather_station",
        type=str,
        help="Weather station to get the measurements from",
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
    # print(args.past_hours)
    past_count_of_15_minutes = args.past_hours * 4
    get_forecast(
        args.location,
        args.weather_station,
        args.hours_to_show,
        past_count_of_15_minutes,
    )
