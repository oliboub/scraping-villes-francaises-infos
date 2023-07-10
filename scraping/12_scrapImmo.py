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

global colonnes
colonnes = ['ville', 'lien','prix_m2','Nombre de logements', "Nombre moyen d'habitant(s) par logement", 'Résidences principales',
            'Résidences secondaires', 'Logements vacants','Maisons', 'Appartements', 'Autres types de logements', 
            'Propriétaires', 'Locataires', '- dont locataires HLM', 'Locataires hébergés à titre gratuit', 
            'Studios', '2 pièces', '3 pièces',   '4 pièces', '5 pièces et plus',
            'Loyer mensuel moyen des logements sociaux', 'Résidences principales de 30 m² à 40 m²',
            'Résidences principales construites de 1919 à 1945', 'Temps passé par des locataires dans leur logement',
            'Résidences principales de 40 m² à 60 m²', 'Résidences principales de 60 m² à 80 m²', 'Logements sociaux vacants',
            'Résidences principales de moins de 30 m²', 'Age moyen des logements sociaux', 'Temps passé en moyenne dans un logement',
            'Résidences principales de 120 m² et plus', 'Temps passé dans un logement à titre gratuit',
            'Evolution du loyer mensuel moyen sur un an', 'Résidences principales construites de 1991 à 2005',
            'Résidences principales construites avant 1919', 'Résidences principales de 100 m² à 120 m²',
            'Résidences principales construites de 1946 à 1970', 'Résidences principales construites de 1971 à 1990',
            'Temps passé par des locataires HLM dans leur logement', 'Logements sociaux loués',
            'Logements sociaux en cours de réhabilitation', 'Temps passé par des propriétaires dans leur logement',
            'Logements sociaux conventionnés', 'Résidences principales construites après 2005',
            'Résidences principales de 80 m² à 100 m²']
# fonction qui va nous permettre de faire la difference entre les elements scrapes et non scrapes

#https://www.journaldunet.com/patrimoine/prix-immobilier/abbenans/ville-25003

#https://www.journaldunet.com/patrimoine/prix-immobilier/blagnac/ville-31069

#colonnes = list(dico.keys())

# fonction qui va nous permettre de faire la difference entre les elements scrapes et non scrapes
def diff(list1, list2):
    return list(set(list1).symmetric_difference(set(list2)))


#changement d'url car site différent
def dataville_url(url):
    urlShort = url[34:].split('/')
    urlVille = urlShort[0]
    urlCode=urlShort[1].split('-')[1]
    #print(urlVille, urlCode)
    newUrl='https://www.journaldunet.com/patrimoine/prix-immobilier/'+urlVille+'/ville-'+urlCode
    return newUrl

global sourceFile
sourceFile = "../dataset/villesTotal.csv"

global targetFile
targetFile= "../dataset/immobilier.csv"

global targetMsg
targetMsg = targetFile.split('/')[2]
#print(targetMsg)

# Parer a l'eventualite que le script s'est arreté
if os.path.isfile(targetFile):
    tableau = pd.read_csv(targetFile, dtype='unicode')
    tableauLiens = pd.read_csv(sourceFile)
    colonnes1 = tableau['lien']
    colonnes2 = tableauLiens['lien']
    listeLiens = diff(colonnes1, colonnes2)
    # realises = len(tableauLiens) - len(tableau) 
    # print(len(tableauLiens),len(tableau),realises)
    # if realises < 0 : realises = 0
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

    newLien = dataville_url(lien)
    reqNewLien = requests.get(newLien)
    time.sleep(2)
    req = requests.get(lien+'/immobilier')
    time.sleep(2)
    
    if req.status_code == 200:
            contenu = req.content
            soup = bs(contenu, "html.parser")
            
            contenu1 = reqNewLien.content
            soup1 = bs(contenu1, "html.parser")
            try:
                prixM2 = soup1.find('span',class_ ='entry--typecsset ccmcss_ft_bold od-price-item').text.split('€')[0].replace('\xa0','')
                #print(prixM2)
                prixM2Float=float(prixM2.strip())
                #print(prixM2Float)
                dico['prix_m2'] = prixM2Float
            except:
                print(lien, "pas de prix au m2")
            
            tables = soup.findAll('table', class_ = "odTable odTableAuto")
            if len(tables) > 0:
                for i in range(len(tables)):
                    for info in tables[i].findAll('tr')[1:]:
                        #print(info)
                        cle = info.findAll('td')[0].text
                        valeur = info.findAll('td')[1].text
                        #print('--->cle',cle,'\tvaleur',valeur)
                        if "Locataires hébergés" in cle:
                            try:
                                dico["Locataires hébergés à titre gratuit"] = float(''.join(valeur.split()).replace(',','.'))
                            except:
                                dico["Locataires hébergés à titre gratuit"] = valeur
                        elif "5 pièces" in cle:
                            try:
                                dico["5 pièces et plus"] = float(''.join(valeur.split()).replace(',','.'))
                            except:
                                dico["5 pièces et plus"] = valeur
                        else:
                            try:
                                dico[cle] = float(''.join(valeur.split()).replace(',','.'))
                            except:
                                dico[cle] = valeur
                                
            #print(dico)
            with open(targetFile, 'a', encoding='utf-8') as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames= colonnes, lineterminator='\n')
                writer.writerow(dico)
                message="["+targetMsg+"] "+ lien
                print(message)
            
    elif req.status_code ==  403:
        print(req.status_code," ",lien,'pid:',os.getpid())
        print("parallelizatrion", Parallelization,' is too high, or timesleep', timeSleep, ' is too low')
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
    with Pool(28) as p:
        p.map(parse, listeLiens)
