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


listeCles = ["ville","lien","Nombre d'entreprises", "- dont commerces et services aux particuliers", "Entreprises créées", "Commerces",
"Services aux particuliers", "Services publics", "Epiceries", "Boulangeries", "Boucheries - charcuteries",
"Librairies - papeteries - journaux", "Drogueries - quincalleries - bricolage", "Banques", "Bureaux de Poste",
"Garages - réparation automobile", "Electriciens", "Grandes surfaces", "Commerces spécialisés alimentaires",
"Commerces spécialisés non alimentaires", "Services généraux", "Services automobiles", "Services du bâtiment", "Autres services",]

dico = {
    **{i : '' for i in listeCles},
    **{str(annee) + " (nbre de creations)": '' for annee in range(2005,2022)},
    **{str(annee) + " (nbre d'entreprises)": '' for annee in range(2005,2022) }
}
global colonnes
colonnes = list(dico.keys())

# fonction qui va nous permettre de faire la difference entre les elements scrapes et non scrapes
def diff(list1, list2):
    return list(set(list1).symmetric_difference(set(list2)))
global sourceFile
sourceFile = "../dataset/villesTotal.csv"

global targetFile
targetFile= "../dataset/entreprises.csv"

global targetMsg
targetMsg = targetFile.split('/')[2]
#print(targetMsg)

# Parer a l'eventualite que le script s'est arreter
if os.path.isfile(targetFile):
    tableau = pd.read_csv(targetFile, dtype='unicode')
    tableauLiens = pd.read_csv(sourceFile)
    colonnes1 = tableau['lien']
    colonnes2 = tableauLiens['lien']
    listeLiens = diff(colonnes1, colonnes2)
    realises = len(tableau) - len(sourceFile)
else:
    tableau = DataFrame(columns = colonnes)
    tableau.to_csv(targetFile, index=False)

    tableauLiens = pd.read_csv(sourceFile)
    listeLiens = tableauLiens['lien']
    realises = 0
    
print('liens restants à traiter:\033[93m\033[1m',len(listeLiens),'\033[0m - liens réalisés: \033[92m\033[1m',realises,'\033[0m')    
    
#listeLiens = [lien for lien in listeLiens if str(lien)[:11] == '/management']

def parse(lien):
    dico = {
            **{i : '' for i in listeCles},
            **{str(annee) + " (nbre de creations)": '' for annee in range(2005,2022)},
            **{str(annee) + " (nbre d'entreprises)": '' for annee in range(2005,2022) }
        }
    
    #print(colonnes)
    dico['lien'] = lien
    dico['ville'] = tableauLiens[tableauLiens['lien'] == lien]['ville'].iloc[0]

    req = requests.get(lien + "/entreprises")
    time.sleep(2.5)
    if req.status_code == 200:
     
            contenu = req.content
            soup = bs(contenu, "html.parser")

            tables = soup.findAll('table', class_ = "odTable odTableAuto")

            if len(tables) > 0:
                for i in range(len(tables)):
                    for info in tables[i].findAll('tr')[1:]:
                        cle = info.findAll('td')[0].text.replace(',',' -') 
                        valeur = info.findAll('td')[1].text
                        try:
                            dico[cle] = float(''.join(valeur.split()))
                        except:
                            dico[cle] = 'nc'

            # Evolution du nombre et de creations d'entreprises
            divs = soup.findAll('div', class_ = 'section-wrapper')
            for div in divs:
                titre_h2 = div.find('h2')
                if titre_h2 != None and "Combien d'entreprises" in titre_h2.text:
                    js_script = div.find('script').string
                    json_data_en = json.loads(js_script)
                    annees = json_data_en['xAxis']['categories']
                    entreprises = json_data_en['series'][0]['data']
                    for annee, entreprise in zip(annees, entreprises):
                        dico[str(annee) + " (nbre d'entreprises)"] = float(entreprise)

                if titre_h2 != None and "Créations d'entreprises" in titre_h2.text:
                    js_script = div.find('script').string
                    json_data_en = json.loads(js_script)
                    annees = json_data_en['xAxis']['categories']
                    creations = json_data_en['series'][0]['data']
                    for annee, creation in zip(annees, creations):
                        dico[str(annee) + " (nbre de creations)"] = float(creation)
            #print(dico)
            with open(targetFile, 'a', encoding='utf-8') as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames= colonnes, lineterminator='\n')
                writer.writerow(dico)
            message="["+targetMsg+"] "+ lien
            print(message)

if __name__ == "__main__":
    with Pool(30) as p:
        p.map(parse, listeLiens)