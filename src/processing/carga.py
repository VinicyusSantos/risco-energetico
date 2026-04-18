import pandas as pd
import glob
import os

RAW_PATH = "data/raw/carga/*.csv"
OUTPUT_PATH = "data/processed/carga_processed.csv"

MAP_SUBSISTEMA = {
    "N": "NORTE",
    "NE": "NORDESTE",
    "S": "SUL",
    "SE": "SUDESTE",
    "SE/CO": "SUDESTE",
    "Sudeste/Centro-Oeste": "SUDESTE",
    "Norte": "NORTE",
    "Nordeste": "NORDESTE",
    "Sul": "SUL"
}


def process():
    files = glob.glob(RAW_PATH)

    dfs = []

    for file in files:
        df = pd.read_csv(file, sep=";", encoding="latin1")

        df.columns = [c.lower() for c in df.columns]

        if "val_cargaenergiamwmed" not in df.columns:
            continue

        df = df[["din_instante", "nom_subsistema", "val_cargaenergiamwmed"]]

        df.columns = ["data", "regiao", "carga"]

        df["data"] = pd.to_datetime(df["data"], errors="coerce")
        df["carga"] = pd.to_numeric(df["carga"], errors="coerce")

        df["regiao"] = df["regiao"].map(MAP_SUBSISTEMA)

        df = df.dropna()

        dfs.append(df)

    df_final = pd.concat(dfs, ignore_index=True)

    df_final = df_final[df_final["data"] >= "2016-01-01"]

    df_final = (
        df_final
        .groupby(["data", "regiao"])
        .agg({"carga": "mean"})
        .reset_index()
    )

    df_final = df_final.sort_values(["regiao", "data"])

    os.makedirs("data/processed", exist_ok=True)
    df_final.to_csv(OUTPUT_PATH, index=False)

    print(df_final.head())
    print("Shape:", df_final.shape)


if __name__ == "__main__":
    process()