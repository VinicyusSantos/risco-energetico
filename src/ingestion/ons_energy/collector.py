import pandas as pd
from src.utils.http import get
from src.utils.logger import get_logger
from io import StringIO

logger = get_logger()

SEARCH_URL = "https://dados.ons.org.br/api/3/action/package_search?q=armazenada"


def search_dataset():
    logger.info("Buscando datasets relacionados à energia armazenada...")

    response = get(SEARCH_URL)
    results = response.json()["result"]["results"]

    if not results:
        raise Exception("Nenhum dataset encontrado")

    for r in results:
        name = r["name"]
        title = r["title"].lower()

        if "subsistema" in title or "ear" in name:
            logger.info(f"Dataset selecionado: {name}")
            return name

    # fallback: pega o primeiro
    logger.warning("Nenhum dataset ideal encontrado, usando o primeiro da lista")
    return results[0]["name"]


def fetch_metadata(dataset_id):
    logger.info(f"Buscando metadados do dataset: {dataset_id}")

    url = f"https://dados.ons.org.br/api/3/action/package_show?id={dataset_id}"
    response = get(url)

    return response.json()["result"]["resources"]


def get_all_csv_urls(resources):
    logger.info("Coletando TODOS os CSVs disponíveis...")

    csv_urls = []

    for r in resources:
        if "csv" in r["format"].lower():
            csv_urls.append(r["url"])

    if not csv_urls:
        raise Exception("Nenhum CSV encontrado")

    logger.info(f"{len(csv_urls)} arquivos CSV encontrados")

    return csv_urls


def download_and_concat(csv_urls):
    logger.info("Baixando e concatenando dados...")

    dfs = []

    for url in csv_urls:
        logger.info(f"Baixando: {url}")

        response = get(url)

        content = response.content.decode("utf-8")

        df = pd.read_csv(
            StringIO(content),
            sep=";",
            encoding="latin1",
            on_bad_lines="skip"
        )

        dfs.append(df)

    final_df = pd.concat(dfs, ignore_index=True)

    logger.info(f"Shape final: {final_df.shape}")

    return final_df


def download_csv(csv_url):
    logger.info("Baixando CSV...")

    response = get(csv_url)

    if not response.content:
        raise Exception("CSV vazio")

    return response.content.decode("utf-8")


def parse_response(csv_content):
    logger.info("Parseando CSV...")

    df = pd.read_csv(
        StringIO(csv_content),
        sep=";",
        encoding="latin1",
        on_bad_lines="skip"
    )

    logger.info(f"Shape: {df.shape}")
    logger.info(f"Colunas: {df.columns.tolist()}")

    return df


def save_raw_data(df):
    df.to_csv("data/raw/ons_ear_latest.csv", index=False)
    logger.info("Dados salvos em data/raw/ons_ear_latest.csv")


def run():
    try:
        dataset_id = search_dataset()
        resources = fetch_metadata(dataset_id)

        csv_urls = get_all_csv_urls(resources)

        df = download_and_concat(csv_urls)

        save_raw_data(df)

        logger.info("Coleta EAR finalizada com sucesso!")

    except Exception as e:
        logger.error(f"Erro no pipeline EAR: {e}")
        raise


if __name__ == "__main__":
    run()