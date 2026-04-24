import pandas as pd
import os

INPUT_PATH = "data/raw/aneel_bandeiras.csv"
OUTPUT_PATH = "data/processed/aneel_bandeiras.csv"

MAP_BANDEIRA = {
    "Verde": 0,
    "Amarela": 1,
    "Vermelha P1": 2,
    "Vermelha P2": 3,
    "Escassez Hídrica": 4
}


def process():
    df = pd.read_csv(INPUT_PATH)

    df.columns = [c.strip().lower() for c in df.columns]

    df = df[["datcompetencia", "nombandeiraacionada"]]

    df.columns = ["data", "bandeira"]

    df["data"] = pd.to_datetime(df["data"], errors="coerce")

    df["bandeira"] = df["bandeira"].str.strip()

    df["bandeira_code"] = df["bandeira"].map(MAP_BANDEIRA)

    df = df.dropna()

    df = df.sort_values("data")

    df = df[df["data"] >= "2016-01-01"]

    df = df.set_index("data").resample("D").ffill().reset_index()

    regioes = ["SUDESTE", "SUL", "NORDESTE", "NORTE"]

    df = df.assign(key=1).merge(
        pd.DataFrame({"regiao": regioes, "key": 1}),
        on="key"
    ).drop("key", axis=1)

    os.makedirs("data/processed", exist_ok=True)
    df.to_csv(OUTPUT_PATH, index=False)

    print(df.head())
    print("Shape:", df.shape)


if __name__ == "__main__":
    process()