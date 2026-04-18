import pandas as pd
import glob
from src.utils.logger import get_logger

logger = get_logger()

RAW_PATH = "data/raw/ena/*.csv"
OUTPUT_PATH = "data/processed/ena_processed.csv"

SUBSISTEMA_MAP = {
    "N": "NORTE",
    "NE": "NORDESTE",
    "S": "SUL",
    "SE": "SUDESTE"
}


def load_files():
    files = glob.glob(RAW_PATH)

    if not files:
        raise Exception("Nenhum arquivo ENA encontrado")

    dfs = []

    for file in files:
        try:
            df = pd.read_csv(
                file,
                sep=";",
                encoding="latin1"
            )
            dfs.append(df)
        except Exception as e:
            logger.warning(f"Erro ao ler {file}: {e}")

    return dfs


def preprocess(df):
    df.columns = [c.strip().lower() for c in df.columns]

    df = df[[
        "ena_data",
        "id_subsistema",
        "ena_bruta_regiao_mwmed"
    ]]

    df.columns = ["data", "subsistema", "ena"]

    df["data"] = pd.to_datetime(df["data"], errors="coerce")

    df["ena"] = (
        df["ena"]
        .astype(str)
        .str.replace(",", ".")
    )

    df["ena"] = pd.to_numeric(df["ena"], errors="coerce")

    df = df.dropna()

    df["subsistema"] = df["subsistema"].str.upper().str.strip()
    df = df[df["subsistema"].isin(SUBSISTEMA_MAP.keys())]
    df["subsistema"] = df["subsistema"].map(SUBSISTEMA_MAP)

    return df


def process_all():
    dfs = load_files()

    all_dfs = []

    for df in dfs:
        try:
            df_clean = preprocess(df)
            all_dfs.append(df_clean)
        except Exception as e:
            logger.warning(f"Erro no preprocess: {e}")

    if not all_dfs:
        raise Exception("Nenhum dado válido após preprocess")

    df_final = pd.concat(all_dfs, ignore_index=True)

    df_final = df_final.drop_duplicates()
    df_final = df_final.sort_values(["subsistema", "data"])

    df_agg = (
        df_final
        .groupby(["data", "subsistema"])
        .agg({
            "ena": "mean"
        })
        .reset_index()
    )

    return df_agg


def save(df):
    df.to_csv(OUTPUT_PATH, index=False)


def run():
    try:
        df = process_all()
        save(df)
    except Exception as e:
        logger.error(f"Erro no processamento ENA: {e}")
        raise


if __name__ == "__main__":
    run()