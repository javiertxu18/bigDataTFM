import src.main.scripts.functions.etl.extraction as extrData
import src.main.scripts.functions.etl.transform as transData
import src.main.scripts.functions.etl.load as loadData
from src.main.scripts.functions import inOut as inOutFunctions
from src.main.scripts.functions import general as generalFunctions

from datetime import datetime
import time

import os
import sys
if os.name != "nt":
    # Preparamos el path para que busque los recursos
    sys.path.append(os.path.abspath(os.getcwd()))
    # print("PATH SYS" + str(sys.path))

if __name__ == '__main__':

    # Configuración inicial
    inOutFunctions.setConfig()

    # Creación del logger
    logger = generalFunctions.getLogger("main")

    # Leemos el fichero config
    conf = inOutFunctions.readConfig()

    logger.info("Inicio de programa")
    while True:
        dtimeInicio = datetime.utcnow()

        print("Ejecutando tarea a las " + str(dtimeInicio) + "UTC")
        logger.info("Ejecutando tarea a las " + str(dtimeInicio) + "UTC")

        # Extraemos los datos
        logger.info("Extraemos los datos")
        scr = extrData.getCryptoData()

        # Transformamos los datos y los guardamos en un dataFrame
        logger.info("Transformamos los datos y los guardamos en un dataFrame")
        scr.resultDataframe = transData.transformCrypto(scr)

        # Los guardamos
        logger.info("Guardamos los datos en parquet con compresión gzip")
        loadData.loadCrypto(conf["DEFAULT"]["res_path"]+str("/out/cryptoData.parquet.gzip"), scr.resultDataframe, "gzip")

        print("Fin tarea a las " + str(datetime.utcnow()) + "UTC")
        print("Tiempo tomado para la tarea: " + str(datetime.utcnow() - dtimeInicio))
        logger.info("Fin tarea a las " + str(datetime.utcnow()) + "UTC")
        logger.info("Tiempo tomado para la tarea: " + str(datetime.utcnow() - dtimeInicio))

        # Dormimos 5 minutos
        print("Dormimos 10 minutos.")
        logger.info("Dormimos 10 minutos")
        time.sleep(600)

    logger.info("Fin de programa")


