# 1 - leggi e carica i datasets
# 2 - concatena horizon
# 3 - scrivi ds (script)
# 4 - genera edgelist ds
# 5 - SCRIVERE MATCH FILE A MANO 
# 6 - genera config files (script)
# 7 - sposta files al posto giusto

import os
import argparse
import pandas as pd
from EmbDI import data_preprocessing as dp

PATH = os.path.dirname(os.path.abspath(__file__))
PATH = PATH.replace("\\", "/")

def launcher(ds1, ds2, separatore, nome_info_file, nome_output_file):

    parameters = {
        "output_file": nome_output_file,
        "concatenate": "horizon",
        "missing_value": "nan,ukn,none,unknown,-",
        "missing_value_strategy": "",
        "round_number": 1,
        "round_columns": "",
        "auto_merge": False,
        "expand_columns": "",
        "tokenize_shared": False,
    }

    # carica il primo ds
    print("lettura datasets...")
    f1 = '{}/pipeline/datasets/{}'.format(PATH, ds1)
    df1 = pd.read_csv(f1, sep = separatore)
    for c in df1.columns:
        if df1[c].dtype == "object":
            df1[c] = df1[c].str.replace("_", " ")

    # carica il secondo ds
    f2 = '{}/pipeline/datasets/{}'.format(PATH, ds2)
    df2 = pd.read_csv(f2, sep = separatore)
    for c in df2.columns:
        if df2[c].dtype == "object":
            df2[c] = df2[c].str.replace("_", " ")

    # preprocessing dei dati
    print("preprocessing e concatenamento dei datasets...")
    df_c = dp.data_preprocessing([df1, df2], parameters)

    # scrittura file info dei ds
    print("salvataggio info file...")
    dp.write_info_file([df1, df2], nome_info_file, [f1, f2])

    # scrittura su disco del ds
    print("salvataggio datasets concatenati...")
    df_c.to_csv(PATH + "/pipeline/datasets/" + parameters["output_file"] + ".csv", index=False)

    # generazione edgelist del dataset concatenato
    print("generazione edgelist...")
    input_edgelist = PATH + "/pipeline/datasets/" + parameters["output_file"] + ".csv"
    output_edgelist = PATH + "/pipeline/er_edgelist/" + nome_output_file + "_edgelist.txt"

    # TODO: aggiustare os.system
    os.system(PATH + "/EmbDI/edgelist.py -i " + input_edgelist + "-o " + output_edgelist)

def parse_args():

    parser = argparse.ArgumentParser()
    parser.add_argument('--ds1', type=str, required=True)
    parser.add_argument('--ds2', type=str, required=True)
    parser.add_argument('--sep', type=str, required=True)
    parser.add_argument('--info', type=str, required=True)
    parser.add_argument('--out', type=str, required=True)

    return parser.parse_args()

if __name__ == "__main__":

    # argv[1] -> primo dataset csv
    # argv[2] -> secondo dataset csv
    # argv[3] -> carattere separatore per il csv
    # argv[4] -> nome info file 
    # argv[5] -> nome output file 

    args = parse_args()
    launcher(args.ds1, args.ds2, args.sep, args.info, args.out)