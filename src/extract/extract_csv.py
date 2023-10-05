import pandas as pd


def extract_csv(csv_path):
    return pd.read_csv(csv_path, header='infer')


def extract_csv_incrementally(csv_path):
    return pd.read_csv(csv_path, header='infer') # Agregar condicion de filtro
