import sys
from src.main.scripts.objects.scrapping.Scrapper import Scrapper
import pandas as pd
from datetime import datetime

if __name__ == '__main__':

    # Inicializamos un objeto de tipo Scrapper
    scr = Scrapper("https://www.estrategiasdeinversion.com/cotizaciones/criptomonedas")

    # Aplicamos sus funciones para sacar los datos que nos interesan
    scr.find("table", "class_", "tbl tbl-cn-3", "soup")
    scr.findAll("tr", "result")

    results = scr.result
    lstResults = []

    timestamp = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')

    colnames = [
        'name',
        'last',
        'variation',
        'variation %',
        'max',
        'min',
        'value time (UTC)',
        'timestamp(UTC)'
    ]

    for row in results[1:]:
        col = list(row.find_all("td"))
        temp = []
        temp.append(str(col[0]).split('">')[1].split("</")[0])
        temp.append(str(col[1]).split('">')[1].split("</")[0].replace(".","").replace(",","."))
        temp.append(str(col[2]).split('">')[1].split("</")[0].replace(",","."))
        temp.append(str(col[3]).split('">')[1].split("</")[0].replace(",",".").replace("%",""))
        temp.append(str(col[4]).split('>')[1].split("</")[0].replace(",","."))
        temp.append(str(col[5]).split('>')[1].split("</")[0].replace(",","."))
        temp.append(str(col[6]).split('\n')[1].split("\n")[0].strip())
        temp.append(str(timestamp))

        print(datetime.strptime(temp[6], '%H:%M:%S'))

        lstResults.append(temp)

    scr.resultDataframe = pd.DataFrame(lstResults, columns=colnames)

    print(scr.resultDataframe)


