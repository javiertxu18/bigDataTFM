import pandas as pd

import src.main.scripts.functions.inOut as inOutFunc
import src.main.scripts.functions.general as generalFunc

# En este .py se guardarán las funciones relacionadas con la carga
# de los datos extraídos ó transformados anteriormente

# Creamos el logger
logger = generalFunc.getLogger("Load")


def loadCryptoToParquet(path, df, compression="gzip"):
    """
    Función para guardar la info en un archivo parquet
    :param path: Ruta a donde se va a guardar el fichero parquet
    :param df: Dataframe que queremos guardar
    :param compression: Tipo de compresión, por defecto Gzip
    :return: True si ha ido bien, False si ha ido mal
    """

    try:
        logger.debug("Cargamos el contenido del parquet en un dataframe")
        print(path)
        dfExtr = pd.read_parquet(path)

        logger.debug("Juntamos los dataframes")
        df = pd.concat([df, dfExtr], axis=0)
    except:
        pass

    logger.info("Guardamos el df extraído y transformado en un parquet.")
    inOutFunc.saveParquet(path, df, compression)
    logger.info("Guardado correctamente.")


def loadCryptoToCsv(path, df):
    """
    Función para guardar la info en un archivo CSV
    :param path: Ruta a donde se va a guardar el fichero cs
    :param df: Dataframe que queremos guardar
    :return:
    """

    try:
        df.to_csv(path, mode='a', header=False, index=False)
    except Exception as e:
        logger.error("Error en load: " + str(e))
