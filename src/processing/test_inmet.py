import pandas as pd

FILE_PATH = "data/raw/inmet_latest.csv"  


def run():
    df = pd.read_csv(FILE_PATH)

    print("\n================ HEAD ================\n")
    print(df.head())

    print("\n================ COLUNAS ================\n")
    print(df.columns.tolist())

    print("\n================ REGIÕES ================\n")
    print(df["regiao"].unique())

    print("\n================ PERÍODO ================\n")
    print(df["data"].min(), "→", df["data"].max())

    print("\n================ TIPOS ================\n")
    print(df.dtypes)

    print("\n================ SHAPE ================\n")
    print(df.shape)

    print("\n================ NULOS ================\n")
    print(df.isnull().sum())

    print("\n================ DESCRIBE ================\n")
    print(df.describe())


if __name__ == "__main__":
    run()