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

def extract_ghcn(gzfilename, outdir):
    print("Extract GHCN...")
    os.makedirs(outdir, exist_ok=True)
    outfilename = op.join(outdir, op.splitext(op.basename(gzfilename))[0])
    print(outfilename)

    with gzip.open(gzfilename, "rb") as gzf:
        with open(outfilename, "wb") as outf:
            outf.write(gzf.read())
    return outfilename


def get_prcp_from_ghcn(filename, outdir, stations_list_file):
    base_stations_list = get_stations_list(stations_list_file)[["GHCN"]].dropna().reset_index().set_index(
        "GHCN")
    # print (base_stations_list)
    print(f"Reading source file {filename}")
    data = pd.read_csv(filename, header=None, usecols=[0, 1, 2, 3],
                       names=["GHCN_STATION", "DATE", "PARAM", "PRCP_GHCN"], index_col=0)
    data = data.loc[data["PARAM"] == "PRCP"].drop(columns=["PARAM"])
    data["DATE"] = pd.to_datetime(data["DATE"], format="%Y%m%d")
    data["PRCP_GHCN"] /= 10.

    # print (data)

    common_ghcn_stations = sorted(set(data.index) & set(base_stations_list.index))
    # print (len(common_ghcn_stations))
    groups = data.loc[common_ghcn_stations].groupby("GHCN_STATION")
    # print (len(groups))
    os.makedirs(outdir, exist_ok=True)
    for g in tqdm(groups):
        wmo_index = base_stations_list.loc[g[0], "WMO_INDEX"]
        # print(wmo_index)
        outdata = g[1][["DATE", "PRCP_GHCN"]].sort_values("DATE")
        # print (outdata)
        outdata.to_csv(op.join(outdir, wmo_index), index=False)
    return op.abspath(outdir)