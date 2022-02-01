import configparser
import sys
import os
import inspect

import pandas as pd
from multipledispatch import dispatch

from src.main.scripts.functions import general as general

# Creamos el logger
logger = general.getLogger("inOutFunc")


# -----------------------------------------------------------------------------------------------
# DESC:
#   Configura el config parser (Para leer y escribir en el fichero config.ini)
#   Configuramos el logger
# Params:
#   No tiene
# Return:
#   Nada
# Llamar a esta función solo desde el main.py
def setConfig(logger_level=30):
    # Preparamos el logger
    general.setLogger()

    # Preparamos el configParser
    try:
        # Control para que se llame a la función únicamente desde main.py
        if inspect.stack()[1][1].split("/")[-1] != "main.py":
            raise "You can only call this method from main.py"

        # Preparamos el configParser
        conf = configparser.ConfigParser()
        # Leemos el fichero config.ini
        conf.read(sys.path[1] + "/config.ini")
        # Escribimos en el configParser (No en el fichero)
        conf['DEFAULT']['so_name'] = os.name
        conf['DEFAULT']['root_path'] = sys.path[1]
        conf['DEFAULT']['config_path'] = conf['DEFAULT']['root_path'] + "/config.ini"
        conf['DEFAULT']['res_path'] = conf['DEFAULT']['root_path'] + "/src/main/res"
        conf['DEFAULT']['logger_level'] = str(logger_level)  # Nivel del logger por defecto

        # Sobreescribimos el fichero y guardamos la info nueva
        with open(conf['DEFAULT']['config_path'], 'w') as configfile:
            conf.write(configfile)
    except Exception as e:
        # Este error lo mostramos por pantalla ya que el logger no está configurado.
        print("Error en setConfig(): " + str(e))
        pass


# -----------------------------------------------------------------------------------------------
# DESC:
#   Devuelve el configParser para que se pueda leer el fichero config.ini cómodamente
# Params:
#   No tiene
# Return:
#   configParser si ha ido bien, False si ha ido mal


def readConfig():
    try:
        # Preparamos el configParser
        conf = configparser.ConfigParser()
        # Leemos el fichero config.ini
        conf.read(sys.path[1] + "/config.ini")
        # Retornamos el configParser
        return conf
    except Exception as e:
        logger = general.getLogger("inOutFunctions")
        logger.error(str(e))
        return False


# -----------------------------------------------------------------------------------------------

@dispatch(str, pd.DataFrame, str)
def saveParquet(path, df, compression="gzip"):
    '''

    Guarda el dataframe en formato parquet y con compresión snappy por defecto

    :param path: Ruta donde se va a guardar el fichero
    :param df: Dataframe que vamos a guardar
    :param compression: Formato de compresión, por defecto gzip
    :return: True si se ha guardado bien, False si no
    '''

    try:
        # Pasamos a parquet con la compresión especificada
        df.to_parquet(path, compression=compression)

        return True
    except Exception as e:
        logger.error("Error al transformar df a parquet: " + str(e))
        return False
        pass
