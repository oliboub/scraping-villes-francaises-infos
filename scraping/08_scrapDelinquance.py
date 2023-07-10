from bs4 import BeautifulSoup as bs 
from multiprocessing import Pool
import pandas as pd 
from pandas import DataFrame 
from pprint import pprint
import json
import requests
import time
import csv
import os
import re
import sys

colonnes = ['ville', 'lien', "Violences aux personnes", "Vols et dégradations", "Délinquance économique et financière", 
            "Autres crimes et délits","Violences gratuites", "Violences crapuleuses", "Violences sexuelles", 
            "Menaces de violence", "Atteintes à la dignité", "Cambriolages","Vols à main armée (arme à feu)", 
            "Vols avec entrée par ruse", "Vols liés à l'automobile", "Vols de particuliers", "Vols d'entreprises",
            "Violation de domicile", "Destruction et dégradations de biens", "Escroqueries, faux et contrefaçons",
            "Trafic, revente et usage de drogues", "Infractions au code du Travail", "Infractions liées à l'immigration", 
            "Différends familiaux", "Proxénétisme","Ports ou détentions d'arme prohibée", "Recels", 
            "Délits des courses et jeux d'argent", "Délits liés aux débits de boisson et de tabac",
            "Atteintes à l'environnement", "Délits liés à la chasse et la pêche", "Cruauté et délits envers les animaux", 
            "Atteintes aux intérêts fondamentaux de la Nation"]

# fonction qui va nous permettre de faire la difference entre les elements scrapes et non scrapes
def diff(list1, list2):
    return list(set(list1).symmetric_difference(set(list2)))

sourceFile = "../dataset/villesTotal.csv"

global targetFile
targetFile= "../dataset/delinquance.csv"

global targetMsg
targetMsg = targetFile.split('/')[2]
#print(targetMsg)

# Parer a l'eventualite que le script s'est arreter
if os.path.isfile(targetFile):
    tableauEntreprises = pd.read_csv(targetFile, dtype='unicode')
    tableauLiens = pd.read_csv(sourceFile)
    colonnes1 = tableauEntreprises['lien']
    colonnes2 = tableauLiens['lien']
    listeLiens = diff(colonnes1, colonnes2)
else:
    tableauEntreprises = DataFrame(columns = colonnes)
    tableauEntreprises.to_csv(targetFile, index=False)

    tableauLiens = pd.read_csv(sourceFile)
    listeLiens = tableauLiens['lien']


def parse(lien):
    dico = {i : '' for i in colonnes}
    dico['lien'] = lien
    dico['ville'] = tableauLiens[tableauLiens['lien'] == lien]['ville'].iloc[0]


    req = requests.get("http://www.linternaute.com/actualite/delinquance/" + lien[34:])
    
    time.sleep(3)
    if req.status_code == 200:
        with open(targetFile, 'a', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames= colonnes, lineterminator='\n')
            contenu = req.content
            soup = bs(contenu, "html.parser")

            tables = soup.findAll('table', class_="odTable odTableAuto")

            if len(tables) > 0:
                for i in range(len(tables)):
                    for info in tables[i].findAll('tr')[1:]:
                        cle = info.findAll('td')[0].text 
                        valeur = info.findAll('td')[1].text
                        try:
                            dico[cle] = float(''.join(valeur.split()).split('c')[0])
                        except:
                            dico[cle] = 'nc'

            writer.writerow(dico)
            message="["+targetMsg+"] "+ lien
            print(message)

if __name__ == "__main__":
    with Pool(28) as p:
        p.map(parse, listeLiens)
