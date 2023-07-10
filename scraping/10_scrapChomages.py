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


dico = {
    **{i : '' for i in ['ville','lien']},
    **{str(annee) : '' for annee in range(2003,2020)}
}

global colonnes
colonnes = list(dico.keys())

# fonction qui va nous permettre de faire la difference entre les elements scrapes et non scrapes
def diff(list1, list2):
    return list(set(list1).symmetric_difference(set(list2)))

global sourceFile
sourceFile = "../dataset/villesTotal.csv"

global targetFile
targetFile= "../dataset/chomage.csv"

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
    realises = len(tableauLiens) -len(tableau)
    if realises < 0 : realises = 0
else:
    tableau = DataFrame(columns = colonnes)
    tableau.to_csv(targetFile, index=False)

    tableauLiens = pd.read_csv(sourceFile)
    listeLiens = tableauLiens['lien']
    realises = 0
    
print('liens restants à traiter:\033[93m\033[1m',len(listeLiens),'\033[0m - liens réalisés: \033[92m\033[1m',realises,'\033[0m')    

def parse(lien):
    dico = {
            **{i : 'nc' for i in ['ville','lien']},
            **{str(annee) : 'nc' for annee in range(2006,2020)}
        }
    dico['lien'] = lien
    #print(lien)
    dico['ville'] = tableauLiens[tableauLiens['lien'] == lien]['ville'].iloc[0]

    
    newLien = lien + "/emploi"
    req = requests.get(newLien)
    
    time.sleep(2.5)
    if req.status_code == 200:
     
        contenu = req.content
        soup = bs(contenu, "html.parser")

                # Evolution du taux de chomage
        divs = soup.findAll('div', class_ = 'section-wrapper')
        #print(len(divs))
        
        for div in divs:
            try:
                titre_h2 = div.find('h2')
            #    print(titre_h2)
                if titre_h2 != None and "Quel est le taux de chômage" in titre_h2.text:
                    #print(titre_h2)
                    js_script = div.find('script').string
                    json_data_en = json.loads(js_script)
                    annees = json_data_en['xAxis']['categories']
                    chomages = json_data_en['series'][0]['data']
                    for annee, chomage in zip(annees, chomages):
                        dico[str(annee)] = float(chomage)
                    #print(dico)
                
            except: 
                print('\033[91m\033[1m[erreur:]\033[0m', newLien)
                
                
        with open(targetFile, 'a', encoding='utf-8') as csvfile:
                    writer = csv.DictWriter(csvfile, fieldnames= colonnes, lineterminator='\n')
                    writer.writerow(dico)
                    message="["+targetMsg+"] "+ newLien
                    print(message)

    elif req.status_code ==  403:
        print(req.status_code," ",lien,'pid:',os.getpid())
        print("parallelizatrion", Parallelization,' is too high, or timesleep', timeSleep, ' is too low')
        print("Wait few minutes before relaunching, like site have probably blocked temporarly your internet adress..")
        sys.exit(1)
        
    elif req.status_code == 404:
        print("200 - pid:",os.getpid(),'\tlien:',newLien,"\033[91m\033[1m(page not found)\033[0m")
        dico['lien'] = lien
        dico['ville'] = tableauLiens[tableauLiens['lien'] == lien1]['ville'].iloc[0]
        #nc_ville(dico['ville'],dico['lien'])

        
    else:
        print(req.status_code, 'code à voir')




if __name__ == "__main__":
    with Pool(28) as p:
        p.map(parse, listeLiens)