import pandas as pd

import src.main.scripts.functions.etl.extraction as extrData
import src.main.scripts.functions.etl.transform as transData
import src.main.scripts.functions.etl.load as loadData
from src.main.scripts.functions import inOut as inOutFunctions
from src.main.scripts.functions import general as generalFunctions

if __name__ == '__main__':
    # Configuración inicial
    inOutFunctions.setConfig()

    # Creación del logger
    logger = generalFunctions.getLogger("main")

    # Leemos el fichero config
    conf = inOutFunctions.readConfig()

    logger.info("Inicio de programa")

    # Extraemos los datos
    logger.info("Extraemos los datos")
    scr = extrData.getCryptoData()

    # Transformamos los datos y los guardamos en un dataFrame
    logger.info("Transformamos los datos y los guardamos en un dataFrame")
    scr.resultDataframe = transData.transformCrypto(scr)

    # Los guardamos
    logger.info("Guardamos los datos en parquet con compresión gzip")
    loadData.loadCrypto(conf["DEFAULT"]["res_path"]+str("/out/cryptoData.parquet.gzip"), scr.resultDataframe, "gzip")

    logger.info("Fin de programa")


