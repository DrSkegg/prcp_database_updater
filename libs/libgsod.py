# !/usr/bin/env python3
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

def extract_gsod(tarfilename, outdir):
    print("Extract GSOD archive...")
    # Создаем временную директорию для файлов gz из тара GSOD
    gsod_temp_dir = tempfile.mkdtemp(prefix="GSOD_")
    # print(gsod_temp_dir)
    # Распаковаываем туда тар

    tarfile.open(tarfilename).extractall(gsod_temp_dir)
    # Распаковываем из временной директории gz файлы в целевую директорию
    os.makedirs(outdir, exist_ok=True)
    for i in tqdm(glob(op.join(gsod_temp_dir, "*.gz"))):
        # print(i)
        # outfilename = op.splitext(op.basename(i))[0]
        outfilename = op.join(outdir, op.splitext(op.basename(i))[0])
        # print(outfilename)
        with gzip.open(i, "rb") as gzf:
            with open(outfilename, "wb") as outf:
                outf.write(gzf.read())
    return op.abspath(outdir)


def _get_prcp_from_gsod(gsodfilename):
    data = pd.read_fwf(gsodfilename,
                       colspecs=[(14, 22), (118, 123)],
                       parse_dates=["YEARMODA"],
                       index_col=["YEARMODA"],
                       na_values=99.99
                       )
    data.index.name = "DATE"
    data.columns = ["PRCP_GSOD"]
    data.re
    data["PRCP_GSOD"] *= 25.4
    return data


def get_prcp_from_gsod(filedir, year, outdir, stations_list_file):
    print("Extracting precipitation data from GSOD")
    base_stations_file_list = get_stations_list(stations_list_file)[["GSOD"]].dropna().apply(lambda x:
                                                                                             x + f"-{year}.op").reset_index().set_index(
        "GSOD")
    # print (set(base_stations_file_list.index))
    files_list = set((op.basename(x) for x in glob(op.join(filedir, "*.op"))))
    common_files = sorted(set(base_stations_file_list.index) & files_list)
    # print(common_files)
    os.makedirs(outdir, exist_ok=True)
    for i in tqdm(common_files):
        outdata = _get_prcp_from_gsod(op.join(filedir, i))
        # outdata.columns = ["DATE", "PRCP_GSOD"]
        outname = op.join(outdir, base_stations_file_list.loc[i, "WMO_INDEX"])
        # outname =  base_stations_file_list.loc[i, "WMO_INDEX"]
        # print(outname)
        outdata.to_csv(outname, float_format="%.1f")
    return op.abspath(outdir)






