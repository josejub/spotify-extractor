import pandas as pd
import sys
import os
import argparse

def join_files(csv_folder, out_path):

    archivos = os.listdir(csv_folder)
    archivos  = [x for x  in archivos if ".csv" in x]

    data = [pd.read_csv(csv_folder + "/" + archivo) for archivo in archivos]

    out = pd.concat(data)
    out.to_csv(out_path, index=False)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-csv_folder", help="folder containing csv files to join")
    parser.add_argument("-out_path", help="output file path for joined csv file")

    args = parser.parse_args()

    join_files(args.csv_folder, args.out_path)
