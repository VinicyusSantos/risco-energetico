import pandas as pd
from src.utils.http import get
from src.utils.logger import get_logger
from io import StringIO

logger = get_logger()

URL = "https://dados.ons.org.br/api/3/action/package_show?id=reservatorio"


def fetch_data():
    logger.info("Buscando metadados do ONS...")

    response = get(URL)
    resources = response.json()["result"]["resources"]

    if not resources:
        raise Exception("Nenhum recurso encontrado no dataset do ONS")

    return resources


def get_csv_url(resources):
    logger.info("Procurando URL do CSV...")

    for r in resources:
        if "csv" in r["format"].lower():
            logger.info(f"CSV encontrado: {r['url']}")
            return r["url"]

    raise Exception("Nenhum CSV encontrado nos recursos")


def download_csv(csv_url):
    logger.info("Baixando CSV do ONS...")

    response = get(csv_url)

    if not response.content:
        raise Exception("CSV vazio")

    return response.content.decode("utf-8")


def parse_response(csv_content):
    logger.info("Parseando CSV...")

    df = pd.read_csv(StringIO(csv_content), sep=";", encoding="latin1")

    logger.info(f"Shape do dataframe: {df.shape}")
    logger.info(f"Colunas do dataframe: {df.columns.tolist()}")

    return df


def validate_schema(df):
    logger.info("Validando schema...")

    expected_columns = df.columns.tolist()

    if len(expected_columns) == 0:
        raise Exception("DataFrame vazio")

    logger.info(f"Colunas encontradas: {expected_columns}")

    return True


def save_raw_data(df):
    file_path = f"data/raw/ons_{pd.Timestamp.now().date()}.csv"

    df.to_csv(file_path, index=False)
    logger.info(f"Dados salvos em {file_path}")


def run():
    try:
        resources = fetch_data()
        csv_url = get_csv_url(resources)
        csv_content = download_csv(csv_url)
        df = parse_response(csv_content)
        validate_schema(df)
        save_raw_data(df)

        logger.info("Coleta do ONS finalizada com sucesso!")

    except Exception as e:
        logger.error(f"Erro no pipeline do ONS: {e}")
        raise


if __name__ == "__main__":
    run()
