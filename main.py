import os
import sys
from loguru import logger

# Importações de Ingestão
from src.ingestion.aneel import collector as aneel_collector
from src.ingestion.carga_demanda import collector as carga_collector
from src.ingestion.INMET import collector as inmet_collector
from src.ingestion.ons_energy import collector as ear_collector

# Importações de Processamento
from src.processing import aneel as aneel_proc
from src.processing import ons_ear as ear_proc
from src.processing import ena as ena_proc
from src.processing import carga as carga_proc
from src.processing import final_df as merge_proc

# Importação de Scripts (Feature Engineering)
import scripts.featureengineering as fe_script

def setup_folders():
    folders = [
        "data/raw/carga",
        "data/raw/ena",
        "data/processed",
        "logs"
    ]
    for folder in folders:
        os.makedirs(folder, exist_ok=True)
        logger.info(f"Pasta verificada: {folder}")

def run_ingestion():
    logger.info("=== INICIANDO FASE DE INGESTÃO ===")
    
    # ENA não tem coletor automatizado, assume-se que os arquivos já estão em data/raw/ena
    # Se houver um coletor futuro, deve ser adicionado aqui.
    
    steps = [
        ("ANEEL", aneel_collector.run),
        ("Carga/Demanda", carga_collector.run),
        ("INMET", inmet_collector.run),
        ("ONS EAR", ear_collector.run),
    ]
    
    for name, func in steps:
        try:
            logger.info(f"Executando coleta: {name}")
            func()
        except Exception as e:
            logger.error(f"Falha na coleta {name}: {e}")

def run_processing():
    logger.info("=== INICIANDO FASE DE PROCESSAMENTO ===")
    
    steps = [
        ("ANEEL", aneel_proc.process),
        ("ONS EAR", ear_proc.run),
        ("ENA", ena_proc.run),
        ("Carga", carga_proc.process),
    ]
    
    for name, func in steps:
        try:
            logger.info(f"Executando processamento: {name}")
            func()
        except Exception as e:
            logger.error(f"Falha no processamento {name}: {e}")

def run_final_pipeline():
    logger.info("=== CONSOLIDAÇÃO E FEATURE ENGINEERING ===")
    
    try:
        logger.info("Mesclando datasets (final_df)...")
        import src.processing.final_df
        
        logger.info("Executando Feature Engineering...")
        import scripts.featureengineering
        
    except Exception as e:
        logger.error(f"Erro na fase final: {e}")

def main():
    logger.add("logs/pipeline.log", rotation="10 MB")
    logger.info("Iniciando Pipeline de Risco Energético")
    
    setup_folders()
    run_ingestion()
    run_processing()
    run_final_pipeline()
    
    logger.info("Pipeline finalizado com sucesso!")

if __name__ == "__main__":
    main()
