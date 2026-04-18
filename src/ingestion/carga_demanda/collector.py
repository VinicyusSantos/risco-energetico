import os
import pandas as pd
from src.utils.http import get
from src.utils.logger import get_logger

logger = get_logger()

CKAN_BASE = "https://dados.ons.org.br/api/3/action/package_search"
QUERY = "carga energia subsistema"

RAW_DIR = "data/raw/carga"
os.makedirs(RAW_DIR, exist_ok=True)


def fetch_dataset_metadata():
    params = {
        "q": QUERY,
        "rows": 5
    }
    logger.info("Buscando datasets no portal ONS...")
    response = get(CKAN_BASE, params=params)
    return response.json()


def extract_csv_urls(metadata):
    urls = []

    for result in metadata["result"]["results"]:
        for resource in result.get("resources", []):
            url = resource.get("url", "")
            if url.endswith(".csv"):
                urls.append(url)

    return urls


def download_csv(url):
    try:
        logger.info(f"Baixando: {url}")
        response = get(url)

        filename = url.split("/")[-1]
        path = os.path.join(RAW_DIR, filename)

        with open(path, "wb") as f:
            f.write(response.content)

        return path

    except Exception as e:
        logger.warning(f"Erro ao baixar {url}: {e}")
        return None


def run():
    metadata = fetch_dataset_metadata()
    urls = extract_csv_urls(metadata)

    if not urls:
        raise Exception("Nenhum CSV encontrado no portal ONS")

    logger.info(f"{len(urls)} arquivos encontrados")

    saved_files = []

    for url in urls:
        path = download_csv(url)
        if path:
            saved_files.append(path)

    logger.info(f"{len(saved_files)} arquivos salvos em {RAW_DIR}")


if __name__ == "__main__":
    run()