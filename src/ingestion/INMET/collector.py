import pandas as pd
from io import StringIO
from datetime import datetime
from src.utils.http import get
from src.utils.logger import get_logger

logger = get_logger()

STATIONS = {
    "SUDESTE": [
        {"code": "A701", "city": "SAO_PAULO", "state": "SP"},
        {"code": "A510", "city": "BELO_HORIZONTE", "state": "MG"},
        {"code": "A621", "city": "RIO_DE_JANEIRO", "state": "RJ"}
    ],
    "SUL": [
        {"code": "A807", "city": "CURITIBA", "state": "PR"},
        {"code": "A869", "city": "PORTO_ALEGRE", "state": "RS"},
        {"code": "A810", "city": "FLORIANOPOLIS", "state": "SC"}
    ],
    "NORDESTE": [
        {"code": "A301", "city": "RECIFE", "state": "PE"},
        {"code": "A404", "city": "SALVADOR", "state": "BA"},
        {"code": "A306", "city": "FORTALEZA", "state": "CE"}
    ],
    "NORTE": [
        {"code": "A101", "city": "MANAUS", "state": "AM"},
        {"code": "A102", "city": "BELEM", "state": "PA"},
        {"code": "A113", "city": "PORTO_VELHO", "state": "RO"}
    ]
}

YEARS = list(range(2016, 2027))

BASE_URL = "https://portal.inmet.gov.br/uploads/dadoshistoricos/{year}/INMET_{region}_{state}_{code}_{city}_01-01-{year}_A_31-12-{year}.CSV"

COLUMN_MAP = {
    "data": "data",
    "precipitacao total": "chuva",
    "temperatura do ar": "temperatura"
}


def build_url(year, region, station):
    region_map = {
        "SUDESTE": "SE",
        "SUL": "S",
        "NORDESTE": "NE",
        "NORTE": "N"
    }

    return BASE_URL.format(
        year=year,
        region=region_map[region],
        state=station["state"],
        code=station["code"],
        city=station["city"]
    )


def fetch_data(url):
    logger.info(f"Baixando: {url}")
    response = get(url)
    return response.text


def parse_response(csv_content):
    try:
        df = pd.read_csv(
            StringIO(csv_content),
            sep=";",
            skiprows=8,
            encoding="latin1"
        )
        return df
    except Exception as e:
        logger.error(f"Erro ao parsear CSV: {e}")
        return None


def preprocess(df, region, station):
    df.columns = [col.strip().lower() for col in df.columns]

    rename_dict = {}
    for col in df.columns:
        for key in COLUMN_MAP:
            if key in col.lower():
                rename_dict[col] = COLUMN_MAP[key]

    df = df.rename(columns=rename_dict)

    required = ["data", "chuva", "temperatura"]
    for col in required:
        if col not in df.columns:
            logger.warning(f"Coluna ausente: {col}")
            return None

    df = df[required]

    df["data"] = pd.to_datetime(df["data"], errors="coerce")
    df["chuva"] = pd.to_numeric(df["chuva"], errors="coerce")
    df["temperatura"] = pd.to_numeric(df["temperatura"], errors="coerce")

    df = df.dropna()

    df["regiao"] = region
    df["estacao"] = station["city"]

    return df


def collect_station(region, station):
    dfs = []

    for year in YEARS:
        try:
            url = build_url(year, region, station)

            raw = fetch_data(url)
            df = parse_response(raw)

            if df is None:
                continue

            df = preprocess(df, region, station)

            if df is None:
                continue

            dfs.append(df)

        except Exception as e:
            logger.warning(f"Erro no ano {year} ({region}-{station['city']}): {e}")
            continue

    if dfs:
        return pd.concat(dfs, ignore_index=True)
    else:
        return None


def collect_all():
    logger.info("Iniciando coleta completa do INMET...")

    all_dfs = []

    for region, stations in STATIONS.items():
        logger.info(f"Processando região: {region}")

        for station in stations:
            df_station = collect_station(region, station)

            if df_station is not None:
                all_dfs.append(df_station)

    if not all_dfs:
        raise Exception("Nenhum dado coletado do INMET")

    df_final = pd.concat(all_dfs, ignore_index=True)

    logger.info(f"Shape final bruto: {df_final.shape}")

    df_agg = (
        df_final
        .groupby(["data", "regiao"])
        .agg({
            "chuva": "mean",
            "temperatura": "mean"
        })
        .reset_index()
    )

    logger.info(f"Shape após agregação: {df_agg.shape}")

    return df_agg


def save_raw_data(df):
    path = f"data/raw/inmet_latest.csv"
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