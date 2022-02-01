from datetime import datetime, timedelta
from multipledispatch import dispatch
from src.main.scripts.objects.scrapping.Scrapper import Scrapper

import pandas as pd


# En este .py se guardarán las funciones relacionadas con la transformación
# de los datos extraídos


@dispatch(Scrapper)
def transformCrypto(scr):
    results = scr.result
    lstResults = []

    timestamp = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')

    colNames = ['name', 'last', 'variation', 'variation %', 'max', 'min', 'value time utc', 'timestamp utc']

    for row in results[1:]:
        col = list(row.find_all("td"))
        temp = []
        # name
        temp.append(str(col[0]).split('">')[1].split("</")[0])
        # last
        temp.append(str(col[1]).split('">')[1].split("</")[0].replace(".", "").replace(",", "."))
        # variation
        temp.append(str(col[2]).split('">')[1].split("</")[0].replace(",", "."))
        # variation %
        temp.append(str(col[3]).split('">')[1].split("</")[0].replace(",", ".").replace("%", ""))
        # max
        temp.append(str(col[4]).split('>')[1].split("</")[0].replace(",", "."))
        # min
        temp.append(str(col[5]).split('>')[1].split("</")[0].replace(",", "."))

        t = str(col[6]).split('\n')[1].split("\n")[0].strip()
        t = datetime(2020, 1, 1, int(t.split(":")[0]), int(t.split(":")[1]), int(t.split(":")[2]))
        t = str(t - timedelta(hours=1)).split(" ")[1]

        # time utc
        temp.append(t)
        # timestamp utc
        temp.append(str(timestamp))

        lstResults.append(temp)

    return pd.DataFrame(lstResults, columns=colNames)
