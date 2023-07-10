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

##Attention certains champs ont des '-' à la place de virgules

global colonnes

colonnes = [
"ville",
"lien",
"Personnes scolarisées 2 - 5 ans",
"Personnes scolarisées 6 - 10 ans",
"Personnes scolarisées 11 - 14 ans",
"Personnes scolarisées 15 - 17 ans",
"Personnes scolarisées 18 - 24 ans",
"Personnes scolarisées 25 - 29 ans",
"Personnes scolarisées 30 ans et plus",
"Personnes non scolarisées Aucun diplôme",
"Personnes non scolarisées Brevet des collèges",
"Personnes non scolarisées CAP / BEP ",
"Personnes non scolarisées Baccalauréat / brevet professionnel",
"Personnes non scolarisées De Bac +2 à Bac +4",
"Personnes non scolarisées Bac +5 et plus",
"Part des femmes Aucun diplôme",
"Part des hommes Aucun diplôme",
"Part des femmes Brevet des collèges",
"Part des hommes Brevet des collèges",
"Part des femmes CAP / BEP ",
"Part des hommes CAP / BEP ",
"Part des femmes Baccalauréat / brevet professionnel",
"Part des hommes Baccalauréat / brevet professionnel",
"Part des femmes De Bac +2 à Bac +4",
"Part des hommes De Bac +2 à Bac +4",
"Part des femmes Bac +5 et plus",
"Part des hommes Bac +5 et plus",
"Ecoles maternelles",
"Ecoles élémentaires",
"Collèges",
"Lycées généraux",
"Lycées professionnels",
"Lycées agricoles",
"Etablissements avec classes préparatoires aux grandes écoles",
"Ecoles de formation sanitaire et sociale",
"Ecoles de commerce, gestion, administration d'entreprises, comptabilité, vente",
"Unités de formation et de recherche (UFR)",
"Instituts universitaires (IUP, IUT et IUFM)",
"Ecoles d'ingénieurs",
"Etablissements de formation aux métiers du sport",
"Centres de formation d'apprentis (hors agricoles)",
"Centres de formation d'apprentis agricoles",
"Autres écoles d'enseignement supérieur",
"Autres formations post-bac non universitaire"]
    
# fonction qui va nous permettre de faire la difference entre les elements scrapes et non scrapes

#https://www.journaldunet.com/patrimoine/prix-immobilier/abbenans/ville-25003

#https://www.journaldunet.com/patrimoine/prix-immobilier/blagnac/ville-31069

#colonnes = list(dico.keys())

# fonction qui va nous permettre de faire la difference entre les elements scrapes et non scrapes
def diff(list1, list2):
    return list(set(list1).symmetric_difference(set(list2)))


#changement d'url car site différent
# def dataville_url(url):
#     urlShort = url[34:].split('/')
#     urlVille = urlShort[0]
#     urlCode=urlShort[1].split('-')[1]
#     #print(urlVille, urlCode)
#     newUrl='https://www.journaldunet.com/patrimoine/prix-immobilier/'+urlVille+'/ville-'+urlCode
#     return newUrl

global sourceFile
sourceFile = "../dataset/villesTotal.csv"

global targetFile
targetFile= "../dataset/education.csv"

global targetMsg
targetMsg = targetFile.split('/')[2]
#print(targetMsg)

global parallelization
parallelization = 15

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
    realises = 0
    
print('liens restants à traiter:\033[93m\033[1m',len(listeLiens)-1,'\033[0m - liens réalisés: \033[92m\033[1m',len(tableau)-1,'\033[0m. Total :\033[95m\033[1m', len(tableauLiens)-1,'\033[0m')

def parse(lien):

    dico = {i : 'nc' for i in colonnes}
    dico['lien'] = lien
    dico['ville'] = tableauLiens[tableauLiens['lien'] == lien]['ville'].iloc[0]

    req = requests.get(lien + "/education")
    time.sleep(3)
    
    if req.status_code == 200:
        
        contenu = req.content
        soup = bs(contenu, "html.parser")

        tables = soup.findAll('table', class_= "odTable odTableAuto")
        for i in range(len(tables)):
            #print(i)
            for table in tables[i].findAll('tr')[1:]: # [1:]:
                #print(table)
                try:
                    #print(table)
                    cle = table.findAll('td')[0].text
                    tempCle = cle
                    if i == 0:
                        cle = 'Personnes scolarisées '+ tempCle
                    if i ==1:                        
                        cle = 'Personnes non scolarisées '+ tempCle
                    if i ==2:
                        cle = 'Part des hommes '+ tempCle
                        cle1 = 'Part des femmes ' + tempCle
                        valeur1 = float(table.findAll('td')[3].text.replace('\xa0','').replace(',','.').split()[0])                        
                        #print('cle1',cle1,'\tvaleur1:',valeur1)
                        dico[cle1] = valeur1
                    valeur = float(table.findAll('td')[1].text.replace('\xa0','').replace(',','.').split()[0])
                    #print('cle',cle,'\tvaleur:',valeur)
                    dico[cle] = valeur
                    
                except:
                    print('-->', lien, 'nothing to do')
                    
            
                
        #for j in dico:
        #    print(j,end =',\n')                
    
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

        
    else:
        print(req.status_code, 'code à voir')

if __name__ == "__main__":
    with Pool(parallelization) as p:
        p.map(parse, listeLiens)
