import pandas as pd
import io
from src.utils.http import get
from src.utils.logger import get_logger

logger = get_logger()

URL = "https://dadosabertos.aneel.gov.br/dataset/7f43a020-6dc5-44b8-80b4-d97eaa94436c/resource/0591b8f6-fe54-437b-b72b-1aa2efd46e42/download/bandeira-tarifaria-acionamento.csv"

OUTPUT_PATH = "data/raw/aneel_bandeiras.csv"


def fetch():
    logger.info("Baixando dados de bandeiras ANEEL...")

    response = get(URL)

    df = pd.read_csv(
        io.BytesIO(response.content),
        sep=";",
        encoding="latin1"
    )

    df.to_csv(OUTPUT_PATH, index=False)

    logger.info(f"Dados salvos em {OUTPUT_PATH}")
    logger.info(f"Shape: {df.shape}")

    return df


def run():
    try:
        fetch()
        logger.info("Ingestion ANEEL finalizado com sucesso!")
    except Exception as e:
        logger.error(f"Erro na ingestion ANEEL: {e}")
        raise


if __name__ == "__main__":
    run()