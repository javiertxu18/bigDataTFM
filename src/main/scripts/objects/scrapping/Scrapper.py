import requests
from bs4 import BeautifulSoup
from multipledispatch import dispatch


class Scrapper:

    def __init__(self, url):
        self.url = url
        self.page = requests.get(url)
        self.__mkBeautiful()
        self.result = None
        self.resultDataframe = None

    # Methods

    # Actualizamos la URL
    def setUrl(self, newUrl):
        self.url = newUrl
        self.page = requests.get(newUrl)
        self.__mkBeautiful()

    # Aplica BeautifulSoup a la página
    def __mkBeautiful(self, parser="html.parser"):
        self.soup = BeautifulSoup(self.page.content, parser)

    # Busca un elemento dentro de la sopa ó de los results, dependiendo del param obj
    @dispatch(str, str, str, str)
    def find(self, typeName, attr, attrName, obj):
        if obj == "soup":
            findInto = self.soup
        else:
            findInto = self.result

        # Si busca una class
        def soupClass():
            self.result = findInto.find(typeName, class_=attrName)

        # Si busca un id
        def soupId():
            self.result = findInto.find(typeName, id=attrName)

        # Si se mete cualquier otra cosa
        def default():
            return -1

        avOpt = {"class_": soupClass, "id": soupId}
        result = avOpt.get(attr, default)
        result()

    # Busca un elemento dentro de la sopa ó de los results, dependiendo del param obj
    @dispatch(str, str)
    def find(self, typeName, obj):
        if obj == "soup":
            findInto = self.soup
        else:
            findInto = self.result

        self.result = findInto.find(typeName)

    # Busca un tipo de elementos dentro de la sopa ó de los results, dependiendo del param obj
    @dispatch(str, str, str, str)
    def findAll(self, typeName, attr, attrName, obj):
        if obj == "soup":
            findInto = self.soup
        else:
            findInto = self.result

        # Si busca una class
        def soupClass():
            self.result = findInto.find_all(typeName, class_=attrName)

        # Si busca un id
        def soupId():
            self.result = findInto.find_all(typeName, id=attrName)

        # Si se mete cualquier otra cosa
        def default():
            return -1

        avOpt = {"class_": soupClass, "id": soupId}
        result = avOpt.get(attr, default)
        result()

    # Busca un tipo de elementos dentro de la sopa ó de los results, dependiendo del param obj
    @dispatch(str, str)
    def findAll(self, typeName, obj):
        if obj == "soup":
            findInto = self.soup
        else:
            findInto = self.result

        self.result = findInto.find_all(typeName)

    # Resetea el atr result
    def resetResult(self):
        self.result = None
