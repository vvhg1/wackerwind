# drop last n rows of hdf5 file

import pandas as pd
import argparse


# args are: hdf5 file name, number of rows to drop
def parse_args():
    parser = argparse.ArgumentParser(description="Get forecast data from Open-Meteo")
    parser.add_argument(
        "-f",
        "--hdf5file",
        type=str,
        help="HDF5 file to drop rows from",
        required=True,
    )
    parser.add_argument(
        "-n",
        "--nrows",
        type=int,
        help="Number of rows to drop",
        required=True,
    )
    return parser.parse_args()


def main():
    args = parse_args()
    with pd.HDFStore(args.hdf5file) as store:
        df = store["data"]
        print("Before")
        print(df.to_string())
        df = df.drop(df.tail(args.nrows).index)
        print("After")
        print(df.to_string())
        store.put("data", df, format="table")


if __name__ == "__main__":
    main()
