#####################################################################################################################################################
# Utility che aiuta nell'utizillo di EmbDI;                                                                                                         #
# questo script genera i file necessari per                                                                                                         #
# effettuare il task di schema matching tramite EmbDI.                                                                                              #
#                                                                                                                                                   #
# Lo script si occupa di:                                                                                                                           #
#                                                                                                                                                   #
#   1 - Caricare due datasets specificati in input                                                                                                  #
#                                                                                                                                                   #
#   2 - Concatenarli secondo la formattazione necessaria                                                                                            #
#       (impostando il parametro "concatenate": "horizon"                                                                                           #
#       nell'oggetto parameters)                                                                                                                    #
#                                                                                                                                                   #
#   3 - Effettuare il preprocessing del dataset concatenato                                                                                         #
#                                                                                                                                                   #
#   4 - Scrivere il nuovo dataset concatenato su disco                                                                                              #
#                                                                                                                                                   #
#   5 - Generare l'info file del nuovo dataset                                                                                                      #
#                                                                                                                                                   #
#   6 - Generare l'edgelist relativa al nuovo ds                                                                                                    #
#                                                                                                                                                   #
#                                                                                                                                                   #
#                                                                                                                                                   #
# E' possibile eseguire questo script seguendo la seguente sintassi:                                                                                #
# py .\embdi_launcher.py --ds1 <primo_dataset.csv> --ds2 <secondo_dataset.csv> --sep "," --info mio_info_file --out mio_dataset_concatenato         #
#                                                                                                                                                   #
# dove:                                                                                                                                             #
#   args.ds1 ->  primo dataset csv                                                                                                                  #
#   args.ds2 ->  secondo dataset csv                                                                                                                #
#   args.sep ->  carattere separatore per il csv                                                                                                    #
#   args.info -> nome info file                                                                                                                     #
#   args.out ->  nome output file                                                                                                                   #
#                                                                                                                                                   #
# Esempio:                                                                                                                                          #
# py .\embdi_launcher.py --ds1 beer/beer-tableA.csv --ds2 beer/beer-tableB.csv --sep "," --info info_beer --out beer_output                         #
#                                                                                                                                                   #
#                                                                                                                                                   #
# NOTA 1: i datasets devono essere presenti nella directory "./pipeline/datasets"                                                                   #
# NOTA 2: i files generati saranno spostati nelle corrette directory di EmbDI automaticamente                                                       #
# NOTA 3: il file matches contenente la ground truth deve essere generata manualmente e inserita in ./pipeline/matches/sm-matches/                  #
#####################################################################################################################################################


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

    # scrittura su disco del ds
    df_c.to_csv(PATH + "/pipeline/datasets/" + parameters["output_file"] + ".csv", index=False)
    print("[" + ds1 + "]" + " concatenato con [" + ds2 + "] scritto su [" + PATH + "/pipeline/datasets/" + parameters["output_file"] + ".csv]")

    # scrittura file info dei ds
    dp.write_info_file([df1, df2], nome_info_file, [f1, f2])
    print("info file scritto su [" + PATH + "/pipeline/info/" + nome_info_file + ".txt]")

    # generazione edgelist del dataset concatenato
    input_edgelist = PATH + "/pipeline/datasets/" + parameters["output_file"] + ".csv"
    output_edgelist = PATH + "/pipeline/er_edgelist/" + nome_output_file + "_edgelist.txt"
    os.system(PATH + "/EmbDI/edgelist.py -i " + input_edgelist + " -o " + output_edgelist)
    print("edgelist scritta su [" + PATH + "/pipeline/er_edgelist/" + nome_output_file + "_edgelist.txt]")

def parse_args():

    parser = argparse.ArgumentParser()
    parser.add_argument('--ds1', type=str, required=True)
    parser.add_argument('--ds2', type=str, required=True)
    parser.add_argument('--sep', type=str, required=True)
    parser.add_argument('--info', type=str, required=True)
    parser.add_argument('--out', type=str, required=True)

    return parser.parse_args()

if __name__ == "__main__":

    args = parse_args()
    launcher(args.ds1, args.ds2, args.sep, args.info, args.out)