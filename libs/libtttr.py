#!/usr/bin/env python3

import sys
import ftplib
import os.path as op
import tarfile
import os
from glob import glob
import gzip
import tempfile
import pandas as pd
import numpy as np
# import progress
import configparser
import datetime
from tqdm import tqdm
from io import StringIO
from libs.libcommon import *


def get_prcp_from_tttr(indir, year, outdir, stations_list_file):
    print("Extracting PRCP data from TTTR")
    stations = set(get_stations_list(stations_list_file).index)
    # print (stations)
    os.makedirs(outdir, exist_ok=True)
    for infile in tqdm(sorted((x for x in glob(op.join(indir, "[0-9]*.txt")) if op.basename(x)[:5] in stations))[:]):

        data = pd.read_csv(infile, header=None, usecols=[1, 2, 3, 11], delimiter=";",
                           dtype={11: str},
                           # na_values='',
                           names=["year", "month", "day", "PRCP_TTTR"])
        year_indices = (data["year"] == year)
        data = data.loc[year_indices, :]
        if year == 1900:
            exclude_wrong_0229_indices = (data["month"] != 2) | (data["day"] != 29)
            data = data[exclude_wrong_0229_indices]

        data["DATE"] = pd.to_datetime(data[["year", "month", "day"]])

        data = data.set_index("DATE")
        data.drop(columns=["year", "month", "day"], inplace=True)
        data["PRCP_TTTR"] = data["PRCP_TTTR"].str.strip()
        # print(data)
        # print(data.info())
        data.loc[f"{year}-01-01":f"{year}-12-31"].to_csv(op.join(outdir, op.basename(infile)[:5]))

    return op.abspath(outdir)

# def


if __name__ == "__main__":
    print ("OK")
    get_prcp_from_tttr("../DATA/TTTR",  "../tmp_files/2016/tttr" , "../DATA/all_stations_in_sources.txt")
