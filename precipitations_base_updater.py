#!/usr/bin/env python3
from libs.libgsod import *
from libs.libghcn import *
from libs.libmakt import *
from libs.libcommon import *
from libs.libtttr import *
# from libs.libfilter import *

import sys
import os.path as op
import configparser
import datetime


def read_config (config_file_name):
    config = configparser.ConfigParser()
    config.optionxform = lambda option: option
    config.read(config_file_name)

    res = {k:config["DEFAULT"][k] for k in config["DEFAULT"].keys()}

    res ["CHECK_FILE_SIZE"] =  (res["CHECK_FILE_SIZE"].upper() == "TRUE")
    res ["GSOD_ONLY_DOWNLOAD"] = (res["GSOD_ONLY_DOWNLOAD"].upper() == "TRUE")
    res ["GHCN_ONLY_DOWNLOAD"] = (res["GHCN_ONLY_DOWNLOAD"].upper() == "TRUE")
    res ["USE_PREPROCESSED_DATA"] = (res["USE_PREPROCESSED_DATA"].upper() == "TRUE")
    res ["USE_TTTR_PREPROCESSED"] = (res["USE_TTTR_PREPROCESSED"].upper() == "TRUE")


    res["GAMMA_FILTER_PROB_BORDER"] = np.float32 (res["GAMMA_FILTER_PROB_BORDER"])
    res["MAX_PRCP"] = np.float32 (res["MAX_PRCP"])

    print ("USE_PREPROCESSED_DATA",  res ["USE_PREPROCESSED_DATA"])

    res["DATA_SOURCES"] = [x for x in res["DATA_SOURCES"].split()]
    res["YEARS"] = {}
    for y in config.sections():
        if config[y]["MONTHS"].lower() == "all":
            res["YEARS"][int(y)] = list (range(1, 13))
        else:
            res["YEARS"][int(y)] = [int(x) for x in config[y]["MONTHS"].split()]

    return res


def base_updater(config_file_name='config.ini'):


    configs = read_config(config_file_name)
    print (configs)
    now = datetime.datetime.now().strftime("%Y-%m-%d %H:%M")
    print ("Now ", now)


    for year in configs["YEARS"].keys():
        # print (root_intermed_dir)
        print("\n" + "=" * 30)
        print("Working with year %d\n" % year)
        root_intermed_dir = configs["INTERMEDIAL_FILES_DIR_PREFIX"].format(year)
        prcp_out_dirs = {}
        months = configs["YEARS"][year]
        if "GSOD" in configs["DATA_SOURCES"] and year >= 1929:
            gsod_prcp_out_dir = op.join(root_intermed_dir,
                                   configs["YEAR_DIR"].format(year=year, now=now),
                                   configs["GSOD_PRCP_DIR"])
            prcp_out_dirs["GSOD"] = gsod_prcp_out_dir
            if not configs["USE_PREPROCESSED_DATA"]:
                print ("Processing GSOD data...")
                gsod_ftp_adress = "ftp://ftp.ncdc.noaa.gov/pub/data/gsod/{0}/gsod_{0}.tar".format(year)

                gsod_tar_file_name = ftp_load_file (gsod_ftp_adress,
                                                    outdir=op.join(root_intermed_dir, configs["GSOD_TAR_DIR"]),
                                                    check_file_size=configs["CHECK_FILE_SIZE"])
                if not configs["GSOD_ONLY_DOWNLOAD"]:
                    gsod_op_dir = extract_gsod(gsod_tar_file_name, outdir=op.join(root_intermed_dir,
                                                configs["YEAR_DIR"].format(year=year, now=now),
                                                configs["GSOD_OP_DIR"]))
                    get_prcp_from_gsod(gsod_op_dir, year=year,
                                       outdir=gsod_prcp_out_dir,
                                       stations_list_file=configs["STATIONS_LIST_FILE"])
                print ("\n"+"-"*30)

        if "GHCN" in configs["DATA_SOURCES"] and year >= 1781:
            ghcn_prcp_out_dir = op.join(root_intermed_dir,
                                            configs["YEAR_DIR"].format(year=year, now=now),
                                                                              configs["GHCN_PRCP_DIR"])
            prcp_out_dirs["GHCN"] = ghcn_prcp_out_dir
            if not configs["USE_PREPROCESSED_DATA"]:
                print ("Processing GHCN data...")
                ghcn_ftp_adress = f"ftp://ftp.ncdc.noaa.gov/pub/data/ghcn/daily/by_year/{year}.csv.gz"
                # print (ghcn_ftp_adress)
                ghcn_gz_file_name = ftp_load_file(ghcn_ftp_adress,
                                                   outdir=op.join(root_intermed_dir, configs["GHCN_GZ_DIR"]),
                                                   check_file_size=configs["CHECK_FILE_SIZE"])
                if not configs["GHCN_ONLY_DOWNLOAD"]:
                    ghcn_csv_dir = extract_ghcn(ghcn_gz_file_name, outdir=op.join(root_intermed_dir,
                                                configs["YEAR_DIR"].format(year=year, now=now),
                                                                                  configs["GHCN_CSV_DIR"]))

                    get_prcp_from_ghcn(ghcn_csv_dir, outdir=ghcn_prcp_out_dir,
                                       stations_list_file=configs["STATIONS_LIST_FILE"]
                                       )
                print("\n" + "-" * 30)



        if "MAKT" in configs["DATA_SOURCES"] and year >= 2012 :

            makt_prcp_out_dir = op.join(root_intermed_dir,
                                        configs["YEAR_DIR"].format(year=year, now=now), configs["MAKT_PRCP_DIR"])
            prcp_out_dirs["MAKT"] = makt_prcp_out_dir
            if not configs["USE_PREPROCESSED_DATA"]:
                print("Processing MAKT data...")

                # print(months)
                dffile = read_makt_files(year, months=months, makt_dir=configs["MAKT_FILES_DIR"],
                                makt_file_name_template=configs["MAKT_FILE_NAME_TEMPLATE"])

                get_prcp_from_makt(dffile, outdir=makt_prcp_out_dir, stations_list_file=configs["STATIONS_LIST_FILE"])
                print("\n" + "-" * 30)

        if "TTTR" in configs["DATA_SOURCES"]:
            print ("TTTR in sources")
            tttr_prcp_out_dir = op.join(root_intermed_dir,
                                        configs["YEAR_DIR"].format(year=year, now=now), configs["TTTR_PRCP_DIR"])
            prcp_out_dirs["TTTR"] = tttr_prcp_out_dir
            if not configs["USE_PREPROCESSED_DATA"]:
                print ("Processing TTTR data...")
                get_prcp_from_tttr(configs["TTTR_FILES_DIR"], year, tttr_prcp_out_dir,
                                   stations_list_file=configs["STATIONS_LIST_FILE"])

                print("\n" + "-" * 30)



        joint_prcp_dir = op.join(root_intermed_dir,
                                            configs["YEAR_DIR"].format(year=year, now=now),
                                                                              configs["JOINT_PRCP_DIR"])
        prcp_out_dirs = [prcp_out_dirs[s] for s in configs["DATA_SOURCES"] if s in prcp_out_dirs.keys()]

        print (prcp_out_dirs)

        join_sources(prcp_out_dirs, year=year, outdir=joint_prcp_dir, months=months, stations_list_file=configs["STATIONS_LIST_FILE"])

        filtered_out_dir = op.join (root_intermed_dir,
                                            configs["YEAR_DIR"].format(year=year, now=now),
                                                                              configs["FILTERED_DIR"])
        print("\n" + "-" * 30)
        gamma_filter(joint_prcp_dir, configs["MEAN_STD_DIR"], filtered_out_dir,
                     configs["GAMMA_FILTER_PROB_BORDER"], configs["MAX_PRCP"] )

        merged_out_dir = op.join (root_intermed_dir,
                                            configs["YEAR_DIR"].format(year=year, now=now),
                                  configs["MERGED_DIR"]                       )
        print("\n" + "-" * 30)
        merge_prcp_sources(filtered_out_dir, merged_out_dir
                           # , configs["DATA_SOURCES"]
                           )

        print("\n" + "-" * 30)

        final_output_dir = op.join (root_intermed_dir, configs["YEAR_DIR"].format(year=year, now=now),
                                  configs["FINAL_OUTPUT_DIR"])

        final_month_output(merged_out_dir, final_output_dir, year)
        
        print ("="*30)

    print ("\nCONGRATULATIONS!!! THE WORK IS DONE!!!")

if __name__ == '__main__':
    if len (sys.argv[1:]) == 0:
        config_file_name = 'config.ini'
    else:
        config_file_name = sys.argv[1]
    base_updater(config_file_name)
