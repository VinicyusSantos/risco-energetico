import pandas as pd
import zipfile
import io
from datetime import datetime
from src.utils.http import get
from src.utils.logger import get_logger

logger = get_logger()

STATIONS = {
    "SUDESTE": [
        "SAO PAULO", "BELO HORIZONTE", "RIO DE JANEIRO"
    ],
    "SUL": [
        "CURITIBA", "PORTO ALEGRE", "FLORIANOPOLIS"
    ],
    "NORDESTE": [
        "RECIFE", "SALVADOR", "FORTALEZA"
    ],
    "NORTE": [
        "MANAUS", "BELEM", "PORTO VELHO"
    ]
}

YEARS = list(range(2016, 2027))

BASE_URL = "https://portal.inmet.gov.br/uploads/dadoshistoricos/{year}.zip"


def fetch_zip(year):
    url = BASE_URL.format(year=year)
    logger.info(f"Baixando ZIP: {url}")
    response = get(url)
    return response.content


def extract_files(zip_content):
    z = zipfile.ZipFile(io.BytesIO(zip_content))
    return z.namelist(), z


def parse_csv(file_name, zip_file):
    if not file_name.lower().endswith(".csv"):
        return None

    try:
        with zip_file.open(file_name) as f:
            content = f.read()

            if len(content) < 1000:
                return None

            df = pd.read_csv(
                io.BytesIO(content),
                sep=";",
                encoding="latin1",
                skiprows=8,
                on_bad_lines="skip"
            )

        return df

    except Exception as e:
        logger.warning(f"Erro ao ler {file_name}: {e}")
        return None


def identify_region(file_name):
    file_name_lower = file_name.lower()

    for region, cities in STATIONS.items():
        for city in cities:
            if city.lower() in file_name_lower:
                return region

    return None


def preprocess(df, file_name):
    df.columns = [c.strip().lower() for c in df.columns]

    region = identify_region(file_name)
    if not region:
        return None

    data_col = next((c for c in df.columns if "data" in c), None)
    chuva_col = next((c for c in df.columns if "precip" in c), None)
    temp_col = next((c for c in df.columns if "temperatura" in c), None)

    if not data_col or not chuva_col or not temp_col:
        return None

    df = df[[data_col, chuva_col, temp_col]]
    df.columns = ["data", "chuva", "temperatura"]

    df["data"] = pd.to_datetime(df["data"], errors="coerce")

    df["chuva"] = (
        df["chuva"]
        .astype(str)
        .str.replace(",", ".")
    )

    df["temperatura"] = (
        df["temperatura"]
        .astype(str)
        .str.replace(",", ".")
    )

    df["chuva"] = pd.to_numeric(df["chuva"], errors="coerce")
    df["temperatura"] = pd.to_numeric(df["temperatura"], errors="coerce")

    # =========================
    # TRATAMENTO INMET
    # =========================
    df["chuva"] = df["chuva"].replace([-9999, -9999.0], pd.NA)
    df["temperatura"] = df["temperatura"].replace([-9999, -9999.0], pd.NA)

    df = df[(df["temperatura"] > -50)]

    df = df.sort_values("data")

    # MISSING VALUES

    # chuva → zero faz sentido físico
    df["chuva"] = df["chuva"].fillna(0)

    # temperatura → interpolação segura
    df["temperatura"] = df["temperatura"].astype(float)
    df["temperatura"] = df["temperatura"].interpolate(
        method="linear",
        limit_direction="both"
    )

    df = df.dropna()

    df["regiao"] = region

    return df


def validate_data(df):
    logger.info("Validando dados...")

    if df["chuva"].min() < 0:
        raise Exception("Valores negativos de chuva detectados")

    if df["temperatura"].min() < -50:
        raise Exception("Temperatura inválida detectada")

    logger.info("Validação OK")


def collect_all():
    logger.info("Iniciando coleta completa do INMET...")

    all_dfs = []

    for year in YEARS:
        try:
            logger.info(f"Processando ano: {year}")

            zip_content = fetch_zip(year)
            file_names, zip_file = extract_files(zip_content)

            for file_name in file_names:
                df = parse_csv(file_name, zip_file)

                if df is None:
                    continue

                df = preprocess(df, file_name)

                if df is None:
                    continue

                all_dfs.append(df)

        except Exception as e:
            logger.warning(f"Erro no ano {year}: {e}")
            continue

    if not all_dfs:
        raise Exception("Nenhum dado coletado do INMET")

    df_final = pd.concat(all_dfs, ignore_index=True)

    logger.info(f"Shape final bruto: {df_final.shape}")

    df_agg = (
        df_final
        .groupby(["data", "regiao"])
        .agg({
            "chuva": "sum",           
            "temperatura": "mean"     
        })
        .reset_index()
    )

    df_agg = df_agg.sort_values(["regiao", "data"])

    df_agg["temperatura"] = (
        df_agg
        .groupby("regiao")["temperatura"]
        .transform(lambda x: x.astype(float).interpolate(limit_direction="both"))
    )

    logger.info(f"Shape após agregação: {df_agg.shape}")

    validate_data(df_agg)

    return df_agg


def save_raw_data(df):
    path = "data/raw/inmet_latest.csv"
    df.to_csv(path, index=False)
    logger.info(f"Dados salvos em {path}")


def run():
    try:
        df = collect_all()
        save_raw_data(df)

        logger.info("Coleta INMET finalizada com sucesso!")

    except Exception as e:
        logger.error(f"Erro na coleta INMET: {e}")
        raise


if __name__ == "__main__":
    run()