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


dico = {
    **{i : '' for i in ['ville','lien','lienChomage']},
    **{str(annee) : '' for annee in range(2003,2022)}
}

global colonnes
colonnes = list(dico.keys())

# changement d'url car site différent
def dataville_url(url):
    urlShort = url[34:].split('/')
    urlVille = urlShort[0]
    urlCode=urlShort[1].split('-')[1]
    #print(urlVille, urlCode)
    newUrl='https://ville-data.com/chomage/'+urlVille+'-'+urlCode
    return newUrl
    

# fonction qui va nous permettre de faire la difference entre les elements scrapes et non scrapes
def diff(list1, list2):
    return list(set(list1).symmetric_difference(set(list2)))

sourceFile = "../dataset/villesTotal.csv"

global targetFile
targetFile= "../dataset/chomage1.csv"

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

#listeLiens = [lien for lien in listeLiens if str(lien)[:11] == '/management']

def parse(lien):
    dico = {
            **{i : '' for i in ['ville','lien']},
            **{str(annee) : '' for annee in range(2003,2022)}
        }
    dico['lien'] = lien
    dico['ville'] = tableauLiens[tableauLiens['lien'] == lien]['ville'].iloc[0]
    lien =dataville_url(lien)
    dico['lienChomage'] = lien
    
    req = requests.get(lien)
    time.sleep(3)
    if req.status_code == 200:
        contenu = req.content
        soup = bs(contenu, "html.parser")

        
        # Evolution du taux de chomage
        #divs = soup.findAll('div', class_ = 'main')
        #https://www.google.com/jsapi
        divs = soup.findAll('script')
        #print(divs)
        a=0        

        for div in divs:
            # print(a)
            if a == 11:
            #    print(a)
            #    print(div,"\n----------------------")
            #    print(type(div))
                div1 = div.text.split('\n')
            #    print(type(div1))
                try:
                    div2 = div1[8]
                    div3 = div2.replace('],[',';').replace('[','').replace(']','').replace(');','').replace("'","").replace(';',',').split(',')
                    #print(type(div3),len(div3),div3)
                    for i in range(1,38,2):
                        #print(lien,div3[i-1])
                        #print(i,float(div3[i]),int(div3[i-1]))
                        dico[str(int(div3[i-1]))]=float(div3[i])
                        #print(lien,div3[i-1])
                except:
                    for i in range(2003,2022):
                        dico[(str(i))]='nc'
            a +=1

        #    pprint(dico)
                            
            
        with open(targetFile, 'a', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames= colonnes, lineterminator='\n')
            writer.writerow(dico)
        print("[data-chomage]", lien)

if __name__ == "__main__":
    with Pool(27) as p:
        p.map(parse, listeLiens)