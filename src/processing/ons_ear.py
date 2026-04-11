import pandas as pd
from src.utils.logger import get_logger

logger = get_logger()

RAW_PATH = "data/raw/ons_ear_latest.csv"

MAIN_PATH = "data/processed/ons_ear_main.csv"
REE_PATH = "data/processed/ons_ear_ree.csv"
PIVOT_PATH = "data/processed/ons_ear_pivot.csv"


def load_data():
    logger.info("Carregando dados brutos...")
    df = pd.read_csv(RAW_PATH)
    return df


def preprocess(df):
    logger.info("Iniciando preprocessamento...")

    # Converter data
    df["ear_data"] = pd.to_datetime(df["ear_data"], errors="coerce")

    # Remover datas inválidas
    df = df.dropna(subset=["ear_data"])

    # Renomear colunas
    df = df.rename(columns={
        "nom_ree": "regiao",
        "ear_data": "data",
        "ear_verif_ree_percentual": "nivel"
    })

    # Filtrar colunas relevantes
    df = df[["regiao", "data", "nivel"]]

    # Remover nulos
    df = df.dropna()

    # Remover valores inválidos (nível deve ser 0–100)
    df = df[(df["nivel"] >= 0) & (df["nivel"] <= 100)]

    # Separar subsistemas (camada principal)
    subsistemas = ["SUDESTE", "SUL", "NORDESTE", "NORTE"]

    df_main = df[df["regiao"].isin(subsistemas)].copy()
    df_ree = df[~df["regiao"].isin(subsistemas)].copy()

    # Garantir tipo correto
    df_main["data"] = pd.to_datetime(df_main["data"])
    df_ree["data"] = pd.to_datetime(df_ree["data"])

    # Ordenar 
    df_main = df_main.sort_values(by=["regiao", "data"])
    df_ree = df_ree.sort_values(by=["regiao", "data"])

    logger.info(f"Main shape: {df_main.shape}")
    logger.info(f"REE shape: {df_ree.shape}")

    return df_main, df_ree


def pivot_main(df_main):
    logger.info("Pivotando dados principais...")

    df_pivot = df_main.pivot(
        index="data",
        columns="regiao",
        values="nivel"
    )

    df_pivot = df_pivot.reset_index()

    df_pivot["data"] = pd.to_datetime(df_pivot["data"])

    logger.info(f"Shape pivotado: {df_pivot.shape}")

    return df_pivot


def save_processed(df_main, df_ree, df_pivot):
    df_main.to_csv(MAIN_PATH, index=False)
    df_ree.to_csv(REE_PATH, index=False)
    df_pivot.to_csv(PIVOT_PATH, index=False)

    logger.info("Dados salvos (main + ree + pivot)")


def run():
    try:
        df = load_data()

        df_main, df_ree = preprocess(df)

        df_pivot = pivot_main(df_main)

        save_processed(df_main, df_ree, df_pivot)

        logger.info("Processamento ONS EAR finalizado com sucesso!")

    except Exception as e:
        logger.error(f"Erro no processamento: {e}")
        raise


if __name__ == "__main__":
    run()