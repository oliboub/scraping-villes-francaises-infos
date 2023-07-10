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


listeCles = ['ville',
             'lien',
             'Salaire moyen des cadres',
             'Salaire moyen des professions intermédiaires',
             'Salaire moyen des employés',
             'Salaire moyen des ouvriers',
             'Salaire moyen des femmes',
             'Salaire moyen des hommes',
             'Salaire moyen des moins de 26 ans',
             'Salaire moyen des 26-49 ans',
             'Salaire moyen des 50 ans et plus',
             'Salaire moyen des cadres femmes',
             'Salaire moyen des professions intermédiaires femmes',
             'Salaire moyen des employés femmes',
             'Salaire moyen des ouvriers femmes',
             'Salaire moyen des cadres hommes',
             'Salaire moyen des professions intermédiaires hommes',
             'Salaire moyen des employés hommes',
             'Salaire moyen des ouvriers hommes',
             'Revenu mensuel moyen par foyer fiscal',
             'Nombre de foyers fiscaux',
             "Nombre moyen d'habitant(s) par foyer"]




dico = {
    **{i : 'nc' for i in listeCles},
    **{str(annee): 'nc' for annee in range(2005,2021)} 
    }

colonnes = list(dico.keys())

# fonction qui va nous permettre de faire la difference entre les elements scrapes et non scrapes
def diff(list1, list2):
    return list(set(list1).symmetric_difference(set(list2)))

global sourceFile
sourceFile = "../dataset/villesTotal.csv"

global targetFile
targetFile= "../dataset/salaires.csv"

global targetMsg
targetMsg = targetFile.split('/')[2]
#print(targetMsg)

global parallelization
parallelization = 5

# Parer a l'eventualite que le script s'est arreté
if os.path.isfile(targetFile):
    tableau = pd.read_csv(targetFile, dtype='unicode')
    tableauLiens = pd.read_csv(sourceFile)
    colonnes1 = tableau['lien']
    colonnes2 = tableauLiens['lien']
    listeLiens = diff(colonnes1, colonnes2)

else:
    tableau = DataFrame(columns = colonnes)
    tableau.to_csv(targetFile, index=False)

    tableauLiens = pd.read_csv(sourceFile)
    listeLiens = tableauLiens['lien']

    
print('liens restants à traiter:\033[93m\033[1m',len(listeLiens)-1,'\033[0m - liens réalisés: \033[92m\033[1m',len(tableau)-1,'\033[0m. Total :\033[95m\033[1m', len(tableauLiens)-1,'\033[0m')

def parse(lien):
    dico = {
        **{i : 'nc' for i in listeCles},
        **{str(annee): 'nc' for annee in range(2006,2016)} 
        }
    dico['ville'] = tableauLiens[tableauLiens['lien'] == lien]['ville'].iloc[0]
    dico['lien'] = lien
    url = "https://www.journaldunet.com/business/salaire/" + lien[34:]
    #print(url)
    req = requests.get(url)
    time.sleep(2)
    if req.status_code == 200:
        
        contenu = req.content
        soup = bs(contenu, "html.parser")

        tables = soup.findAll('table', class_= "odTable odTableAuto")

        if len(tables) > 0:
            for i in range(len(tables)):
                for info in tables[i].findAll('tr')[1:]:
                    #print(i, info)
                    cle = info.findAll('td')[0].text 
                    valeur = info.findAll('td')[1].text
                    try:
                        if 'foyers' in cle:
                            dico[cle] = float(''.join(valeur.split()).split('f')[0])
                        elif 'Salaire' in cle and i == 0:
                            dico[cle] = float(''.join(valeur.split()).split('€')[0])
                        elif 'Salaire' in cle and i == 3:
                            cle1 = cle + " femmes"
                            cle2 = cle + " hommes"
                            #print(cle1)
                            dico[cle1] = float(''.join(valeur.split()).split('€')[0])
                            valeur1 = info.findAll('td')[2].text
                            dico[cle2] = float(''.join(valeur1.split()).split('€')[0])
                        elif 'Salaire' in cle:
                            dico[cle] = float(''.join(valeur.split()).split('€')[0])
                        elif "d'habitant(s)" in cle:
                            dico[cle] = float(''.join(valeur.split()).split('p')[0].replace(',', '.'))
                        elif "Revenu" in cle:
                            dico[cle] = float(''.join(valeur.split()).split('€')[0])
                        else:
                            dico[cle] = float(''.join(valeur.split()))

                    except:
                        dico[cle] = 'nc'

        divs = soup.findAll('div', class_ = 'section-wrapper')
        for div in divs:
            titre_h2 = div.find('h2')
            if titre_h2 != None and "Evolution des revenus" in titre_h2.text:
                js_script = div.find('script').string
                json_data = json.loads(js_script)
                annees = json_data['xAxis']['categories']
                salaires = json_data['series'][0]['data']
                for annee, salaire in zip(annees, salaires):
                    dico[str(annee)] = salaire
        
        
        #for j in dico:
        #    print('[',j,']',dico[j])
        
        with open(targetFile, 'a', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames= colonnes, lineterminator='\n')
            writer.writerow(dico)
            message="["+targetMsg+"] "+ lien
            print(message)
        
    elif req.status_code ==  403:
        print(req.status_code," ",lien,'pid:',os.getpid())
        print("parallelizatrion", parallelization,' is too high, or timesleep is too low')
        print("Wait few minutes before relaunching, like site have probably blocked temporarly your internet adress..")
        sys.exit(1)
        
    elif req.status_code == 404:
        print("200 - pid:",os.getpid(),'\tlien:',lien,"\033[91m\033[1m(page not found)\033[0m")
        dico['lien'] = lien
        dico['ville'] = tableauLiens[tableauLiens['lien'] == lien]['ville'].iloc[0]
        #nc_ville(dico['ville'],dico['lien'])

if __name__ == "__main__":
    with Pool(parallelization) as p:
        p.map(parse, listeLiens)