import pandas as pd

ons = pd.read_csv("data/processed/ons_ear_main.csv")
inmet = pd.read_csv("data/raw/inmet_latest.csv")
ena = pd.read_csv("data/processed/ena_processed.csv")
carga = pd.read_csv("data/processed/carga_processed.csv")
aneel = pd.read_csv("data/processed/aneel_bandeiras.csv")

for df in [ons, inmet, ena, carga, aneel]:
    df["data"] = pd.to_datetime(df["data"])

ena = ena.rename(columns={"subsistema": "regiao"})

df = ons.merge(
    inmet,
    on=["data", "regiao"],
    how="inner"
)

df = df.merge(
    ena,
    on=["data", "regiao"],
    how="inner"
)

df = df.merge(
    carga,
    on=["data", "regiao"],
    how="inner"
)

df = df.merge(
    aneel,
    on=["data", "regiao"],
    how="inner"
)

df = df.sort_values(["regiao", "data"])

print(df.head())

print("\nShape:")
print(df.shape)

print("\nNulos:")
print(df.isnull().sum())

print("\nPeríodo:")
print(df["data"].min(), "→", df["data"].max())

print("\nRegiões:")
print(df["regiao"].unique())

df.to_csv("data/processed/dataset_final.csv", index=False)