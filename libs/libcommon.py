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
import time
from tqdm import tqdm
from scipy.stats import gamma


class file_writer:
    def __init__(self, ftp_host, server_file_name, outfilename):
        self.ftp_host = ftp_host
        self.server_file_name = server_file_name
        self.outfilename = outfilename
        self.ftp_host.sendcmd("TYPE I")
        self.server_file_size = ftp_host.size(self.server_file_name)
        self.progress = tqdm(total=self.server_file_size, unit="b", unit_divisor=1024, unit_scale=1, colour='GREEN')
        self.outfile = open (self.outfilename, "wb")

    def __enter__(self):
        return self

    def __exit__(self, a, b, c):
        self.outfile.close()
        self.progress.close()
        # print ("Writer exited")
        return False

    def write(self, data):
        self.progress.update(len(data))
        self.outfile.write (data)

    def __del__ (self):
        self.outfile.close()
        self.progress.close()
        # print ("Writer closed")


def check_file_size_(ftp, serverfilename, hostfilename):
    ftp.sendcmd("TYPE I")
    return ftp.size(serverfilename) == op.getsize(hostfilename)


def ftp_load_file(filepath, outdir='.', check_file_size=True):
    target_file = op.basename(filepath)
    outfilename = op.join(outdir, target_file)
    # print(target_file)
    server, path = op.dirname(filepath).split("//")[1].split("/", maxsplit=1)
    # print(server)
    # print(path)
    host = ftplib.FTP(server)
    host.login()
    host.cwd(path)
    # print (host.nlst())
    if not (op.exists(outfilename) and check_file_size and check_file_size_(host, target_file, outfilename)):
        print("Loading file %s from %s..." % (target_file, server))
        os.makedirs(outdir, exist_ok=True)
        with file_writer (host, target_file, outfilename) as filewriter:
            host.retrbinary("RETR " + target_file, filewriter.write)
            # time.sleep (1)
            # del (filewriter)
        print("Done")
    else:
        print("Loading file %s from %s is not required" % (target_file, server))
    return op.abspath(outfilename)


def join_sources(prcp_source_dirs, year, outdir, months, stations_list_file):
    print ("Join PRCP from different sources")
    # print ("SORCE DIRS: ", prcp_source_dirs)
    base_stations_list = list(get_stations_list(stations_list_file).index)
    os.makedirs(outdir, exist_ok=True)
    tmp = pd.DataFrame(index=pd.date_range(start=f"{year}-01-01", end=f"{year}-12-31"))
    tmp.index.name = "DATE"

    months_indices = (tmp.index.month.isin(months))
    # tmp = tmp.loc[months_indices]
    # print (months_indices)

    for wmo in tqdm(base_stations_list, colour='BLUE'):
    # for wmo in base_stations_list:

        all_data = [tmp]

        for d in prcp_source_dirs:
            infile = op.join(d, wmo)
            # print(infile)
            if op.exists(infile):

                data = pd.read_csv(infile, index_col="DATE", parse_dates=["DATE"])
                all_data.append(data)
        all_data = pd.concat(all_data, axis=1).loc[months_indices]
        # all_data.dropna(axis=1, inplace=True, how="all")
        all_data.to_csv(op.join(outdir, wmo))
    return op.abspath(outdir)


def gamma_filter_borders (mean_std_file, prob=0.995, max_prcp = 2000.):
    mean_std= pd.read_csv(mean_std_file)
    a = (mean_std["PRCP_MEAN"] ** 2) / (mean_std["PRCP_STD"] ** 2)
    scale = (mean_std["PRCP_STD"] ** 2) / mean_std["PRCP_MEAN"]
    bounds = gamma.ppf(q=prob, a=a, scale=scale, loc=0)
    nan_index = np.isnan(bounds)
    bounds[nan_index] = np.inf
    bounds = np.where (  bounds <  max_prcp, bounds, max_prcp)
    return pd.DataFrame(bounds, index=mean_std["MD"], columns=["BOUNDS"])





def gamma_filter (indir, mean_std_dir, outdir, prob=0.995, max_prcp = 2000.):
    print ("Filtering PRCP joint data")
    os.makedirs(outdir, exist_ok=bool)
    for infile in tqdm (sorted (glob (op.join (indir, "*"))), colour='BLUE'):
        wmo = op.basename(infile)
        data = pd.read_csv(infile)
        try:
            with np.errstate(invalid='ignore'):
              bounds = gamma_filter_borders(op.join(mean_std_dir, wmo) , prob, max_prcp)


            data ["MD"] = data["DATE"].str[5:10]
            data = data.merge(bounds, on="MD")
            data.iloc[:, 1:-2] = np.where(((data.iloc[:, 1:-2].values <= data["BOUNDS"].values.reshape(-1,1)) &
                                           (data.iloc[:, 1:-2].values >=0) )   ,
                                           np.abs(data.iloc[:, 1:-2].values), np.nan )

            outdata = data.iloc[:, :-2]
        except Exception:
            outdata = np.where(data.iloc[:, 1:-2].values >=0  ,
                                           np.abs(data.iloc[:, 1:-2].values), np.nan )
        outdata.to_csv (op.join(outdir, wmo), index=False)


    return op.abspath(outdir)


def merge_prcp_sources (indir, outdir):
    print ("Merging PRCP sources")
    os.makedirs(outdir, exist_ok=True)

    for infile in tqdm(sorted(glob(op.join(indir, "*"))), colour='BLUE'):
        outfile = op.join(outdir, op.basename(infile))

        data = pd.read_csv(infile,
                           index_col="DATE",
                           #                        parse_dates=["DATE"]
                           )
        outdata = select_source(data)
        outdata.to_csv(outfile)
    return op.abspath(outdir)

def _select(x):
    if x.shape[1] <= 1:
        return x

    r = np.where(np.isnan(x[:, 0]), x[:, 1], x[:, 0])
    if x.shape[1] == 2:
        return r
    else:
        r = np.column_stack((r, x[:, 2:]))

    return _select(r)


def select_source(x):
    if x.shape[1] == 0:
        x["PRCP"] = np.nan
        return x
    # new_names = [c for c in sources if c in x.columns]
    # #     print (new_names)
    # x = x[new_names]
    #     print (x)
    r = _select(x.values)
    return pd.DataFrame(r, index=x.index, columns=["PRCP"])

def get_stations_list(filename):
    data = pd.read_csv(filename, dtype=str).set_index("WMO_INDEX")
    # return {x[0]: dict(x[1].dropna()) for x in data.iteritems()}
    return data

def final_month_output (indir, outdir, year):
    print ("Final PRCP output processing")
    output_dir_bin = op.join(outdir, "bin")
    output_dir_csv = op.join(outdir, "csv")
    output_dir_sum = op.join(outdir, "sum")
    os.makedirs(output_dir_bin, exist_ok=True)
    os.makedirs(output_dir_csv, exist_ok=True)
    os.makedirs(output_dir_sum, exist_ok=True)
    all_data = []
    print ("  Reading PRCP merged files")
    for infile in tqdm(sorted(glob(op.join(indir, "*"))), colour='BLUE'):
        # print (infile)
        data = pd.read_csv(infile, parse_dates=["DATE"], index_col="DATE")
        if data.shape[1] != 1:
            # print (op.basename(infile))
            data["PRCP"] = np.nan
        data.rename(columns={"PRCP":op.basename(infile)}, inplace=True)
        all_data.append(data)
    # print (len(all_data))
    all_data = pd.concat(all_data, axis=1)

    print ("Writing final month PRCP files")
    all_data["Y"] = data.index.year
    all_data["M"] = data.index.month
    all_data["D"] = data.index.day



    # print (all_data)
    # print (all_data.info())

    gr = all_data.groupby("M")

    months = []
    for g in tqdm(gr, colour='RED'):
        # print (g)
        month = g[0]
        months.append(month)
        output_csv = op.join(output_dir_csv, "{0}_{1:02}.csv".format (year, month))
        output_bin = op.join(output_dir_bin, "{0}_{1:02}.bin".format (year, month))
        output_sum = op.join(output_dir_sum, "{0}_{1:02}.csv".format (year, month))


        # print (output_csv, output_bin)
        outdata = g[1].iloc[:, :-3].T
        outdata.index.name = "WMO_INDEX"
        outdata.to_csv (output_csv)

        out_month_sum = outdata.sum (axis=1, skipna=False).round(1)
        # out_month_sum.columns.name = [f"PRCP_{year}_{month:02}"]
        out_month_sum.to_csv(output_sum, header=[f"PRCP_{year}_{month:02}"])

        # print (g[1].columns)
        columns_out = list(g[1].columns[-3:]) + list (g[1].columns[:-3])
        # print (columns_out)

        outdata=g[1].loc[:, columns_out]
        outdata.iloc[:, 3:] *= 10.
        outdata.fillna (32768., inplace=True)
        outdata = outdata.values.astype(np.int16)
        outdata.ravel().tofile(output_bin)
        # print (outdata)
    print (f"\nOutput files for year {year} month {months} are created")


if __name__ == "__main__":
    print ("OK")
    final_month_output("../tmp_files/2020/03_merged_prcp", "../tmp_files/2020/04_final_files_test", 2020)
