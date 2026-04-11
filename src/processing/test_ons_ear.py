import pandas as pd

MAIN_PATH = "data/processed/ons_ear_main.csv"
REE_PATH = "data/processed/ons_ear_ree.csv"
PIVOT_PATH = "data/processed/ons_ear_pivot.csv"


def test_main():
    print("\n================ MAIN (SUBSISTEMAS) ================\n")

    df = pd.read_csv(MAIN_PATH)

    print("HEAD:")
    print(df.head())

    print("\nCOLUNAS:")
    print(df.columns.tolist())

    print("\nREGIÕES:")
    print(df["regiao"].unique())

    print("\nPERÍODO:")
    print(df["data"].min(), "→", df["data"].max())

    print("\nTIPOS:")
    print(df.dtypes)

    print("\nSHAPE:")
    print(df.shape)


def test_ree():
    print("\n================ REE (DETALHADO) ================\n")

    df = pd.read_csv(REE_PATH)

    print("HEAD:")
    print(df.head())

    print("\nEXEMPLOS DE REEs:")
    print(df["regiao"].unique()[:10])

    print("\nTOTAL DE REEs:")
    print(len(df["regiao"].unique()))

    print("\nPERÍODO:")
    print(df["data"].min(), "→", df["data"].max())

    print("\nSHAPE:")
    print(df.shape)

def test_pivot():
    print("\n================ PIVOT (DETALHADO) ================\n")
    df_pivot = pd.read_csv(PIVOT_PATH, parse_dates=["data"])

    print("\nHEAD:")
    print(df_pivot.head())
    print("\nSHAPE")
    print(df_pivot.shape)
    print("\nDESCRIBE")
    print(df_pivot.describe())
    print("\nTIPOS:")
    print(df_pivot.dtypes)
    print("\nNULOS:")
    print(df_pivot.isna().sum())


def run():
    test_main()
    test_ree()
    test_pivot()


if __name__ == "__main__":
    run()