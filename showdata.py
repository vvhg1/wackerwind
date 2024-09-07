import os
import pandas as pd
import argparse


def show_data(location, model, hour):
    save_path = "/home/vhg/repos/wackerwind/data/forecasts/"
    if os.path.exists(f"{save_path}{location}_{model}_{hour}.h5"):
        with pd.HDFStore(f"{save_path}{location}_{model}_{hour}.h5") as store:
            temp_df = store["data"]
            print(temp_df.to_string())


def parse_args():
    parser = argparse.ArgumentParser(description="Get forecast data from Open-Meteo")
    parser.add_argument(
        "-l",
        "--location",
        type=str,
        help="Location to get the forecast for, wac, fal",
        required=True,
    )
    parser.add_argument(
        "-s",
        "--hour_to_show",
        type=int,
        help="Hour to show in the forecast",
        required=False,
        default=36,
    )
    parser.add_argument(
        "-m",
        "--model",
        type=str,
        help="Model to show data for, arome_france_hd, icon_d2, metno_seamless",
        required=True,
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    show_data(args.location, args.model, args.hour_to_show)
