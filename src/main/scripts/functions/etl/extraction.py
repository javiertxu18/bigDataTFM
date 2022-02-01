from multipledispatch import dispatch

from src.main.scripts.objects.scrapping.Scrapper import Scrapper


# En este .py se guardarán las funciones relacionadas con la extracción de datos


@dispatch()
def getCryptoData():
    """
    Función para recoger los datos de la url

    :param: No param

    :return: Scrapper
    """

    # Inicializamos un objeto de tipo Scrapper
    scr = Scrapper("https://www.estrategiasdeinversion.com/cotizaciones/criptomonedas")

    # Aplicamos sus funciones para sacar los datos que nos interesan
    scr.find("table", "class_", "tbl tbl-cn-3", "soup")
    scr.findAll("tr", "result")

    return scr
