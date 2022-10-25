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
from libs.libcommon import *
from io import StringIO

def get_prcp_from_makt(filename, outdir, stations_list_file="all_stations_in_sources.txt"):
    print ("Parsing MAKT files...")
    data = pd.read_fwf(filename, colspecs=[(0, 8), (9, 14), (188, 195)], header=None,
                       # nrows=100000
                       names=["DATE", "WMO_INDEX", "PRCP_MAKT"],
                       dtype={"PRCP_MAKT": np.float32, "DATE": str, "WMO_INDEX": str},
                       na_values=-9999.
                       )

    data["DATE"] = pd.to_datetime(data["DATE"].str.replace(" ", "0"), format="%Y%m%d")
    data['WMO_INDEX'] = data["WMO_INDEX"].str.replace(" ", "0").apply(lambda x: "{0:0>5}".format(x))
    # data.columns = ["DATE", "WMO_INDEX", "PRCP_MAKT"]
    data.set_index("WMO_INDEX", inplace=True)
    base_stations_list = get_stations_list(stations_list_file)["MAKT"].dropna()
    common_makt_stations = set(base_stations_list.index) & set(data.index)
    data = data.loc[common_makt_stations]
    # data["PRCP_MAKT"] = data["PRCP_MAKT"] / 100.
    data.loc[data["PRCP_MAKT"] > 9990, "PRCP_MAKT"] = 0.
    data["PRCP_MAKT"] /= 100.
    data["PRCP_MAKT"] = data["PRCP_MAKT"].round(1)

    # print(data)
    # print(data.info())
    groups = data.groupby("WMO_INDEX")
    os.makedirs(outdir, exist_ok=True)
    print ("Extracting and writing PREC form MAKT...")
    for g in tqdm(groups):
        # print (g[0])
        g[1].sort_values("DATE").to_csv(op.join(outdir, g[0]), index=False)

    return op.abspath(outdir)

def read_makt_files (year, months, makt_dir="MAKT", makt_file_name_template = "{year}_{month:02}_P[12].txt"):
    print ("Reading MAKT files...")
    makt_files = []
    for month in months:
        makt_files.extend(glob (op.join(makt_dir, makt_file_name_template.format(year=year, month=month))))
        # makt_text = "".join(makt_text, open())
    makt_files = sorted (makt_files)
    makt_text = ""
    for makt_file in tqdm(makt_files):
        with open (makt_file, "r") as f:
            makt_text = "".join ((makt_text, f.read().strip()+"\n"))
    # print ("MAKT FILES", makt_files)
    # print (makt_text)

    res = StringIO(makt_text)
    # data = pd.read_fwf(res, colspecs=[(0, 8), (9, 14), (188, 195)], header=None,
    #                    # nrows=100000
    #                    names=["DATE", "WMO_INDEX", "PRCP_MAKT"],
    #                    dtype={"PRCP_MAKT": np.float32, "DATE": str, "WMO_INDEX": str},
    #                    na_values=-9999.
    #                    )
    # print (data)
    return res

