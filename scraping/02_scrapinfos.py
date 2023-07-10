import requests
from multiprocessing import Pool
from bs4 import BeautifulSoup as bs
import pandas as pd
from pandas import DataFrame
import csv
from pprint import pprint
import os
import time
import sys
from datetime import datetime

if sys.platform in ('win32', 'msys', 'cygwin'):
    print("Script works only on *NIX type operating systems.")
    sys.exit(1)
# on travaille sur une page et si c'est bon, on itère avec les autres.
#pour trouver les liens, faire clique droit à l'endroit du lien, sur la page internet, et inspecter.
# vérifier la page, et voir si c'est une table. les tables ont le meme nom sur la page JDN, docn c'est plus ismple.
#le nom = 'odTable odTableAuto'

###########################
# Modifs
# mutliprocessing
############################

#fonction qui va faire une difference entre liensVilles.csv et infos.csv
def diff(list1, list2):
    return list(set(list1).symmetric_difference(set(list2))) # set retire les doublons


### Function to check remaining links to process
def check_remaining():
    #source="../dataset/villes1.csv"
    
    global tableauLiens
    if os.path.isfile(targetFile):
        tableauInfos = pd.read_csv(targetFile,encoding='utf-8')
        colonne1 = tableauInfos['lien']
        colonne2 = tableauLiens['lien']
        listeLiens = diff(colonne1,colonne2)
        listeLiens.sort()
    
    else:
        # Creation de notre csv infos
        tableauInfos = DataFrame(columns=colonnes)
        tableauInfos.to_csv(targetFile,index=False)
        listeLiens = tableauLiens['lien']
    
    print("Qtt restant",len(listeLiens),"pid:",os.getpid())
    return(listeLiens) 

### Function to scrap links (liens)
def parse(lien):

    #dico = { i : '' for i in colonnes }
    req = requests.get(lien)
    time.sleep(timeSleep)
    
    if req.status_code == 200:
        print("200 - pid:",os.getpid(),'\tlien:',lien)

        with open("../dataset/infos.csv",'a',encoding='utf-8') as csvfile:
            writer=csv.DictWriter(csvfile,fieldnames=colonnes,lineterminator='\n')
            contenu=req.content
            soup = bs(contenu,"html.parser")
            
            dico['lien'] = lien
            dico['ville'] = tableauLiens[tableauLiens['lien'] == lien]['ville'].iloc[0]

            tables=soup.findAll('table',class_ = 'odTable odTableAuto')


            for i in range (len(tables)): # dépend du nombre de tables qu'il va trouver dans chaque ville, car ça peut fluctuer
                tousLesTr = tables[i].findAll('tr')
            #    dico['lien'] = lien 
                for tr in tousLesTr[1:]: # affiche à partir de l'index 1 et evite le premier tr
                    cle = tr.findAll('td')[0].text
                    valeur = tr.findAll('td')[1].text
                    if "Nom des habitants" in cle:
                        dico['Nom des habitants'] = valeur
                    elif "Taux de chômage" in cle:
                        dico["Taux de chômage (2019)"] = valeur 
                    else:
                        dico[cle] = valeur
#            pprint(dico['lien'])
            writer.writerow(dico)
    else:
        print(req.status_code," ",lien,'pid:',os.getpid())
        print("parallelizatrion", Parallelization,' is too high, or timesleep', timeSleep, ' is too low')
        print("Wait few minutes before relaunching, like site have probably blocked temporarly your internet adress..")
        sys.exit(1)
        
def main():
    ##############################
    # #Variables
    global startTime
    startTime= datetime.utcnow()
    
    global colonnes
    colonnes = ['ville','lien',"Région","Département","Etablissement public de coopération intercommunale (EPCI)","Code postal (CP)","Code Insee","Nom des habitants","Population (2020)","Population : rang national (2020)",
            "Densité de population (2020)","Taux de chômage (2019)","Pavillon bleu","Ville d'art et d'histoire","Ville fleurie","Ville internet","Superficie (surface)","Altitude min.","Altitude max.","Latitude","Longitude"]

    global dico
    dico = {i : '' for i in colonnes}
    
    sourceFile="../dataset/villes100.csv"
    
    global targetFile
    targetFile="../dataset/infos.csv"
    
    global tableauLiens
    tableauLiens = pd.read_csv(sourceFile,encoding='utf-8')       

    listeLiens=check_remaining()
    
    global timeSleep
    timeSleep =2
    
    global Parallelization
    Parallelization = 26
    ##############################
   
    #Launch sraping
    with Pool (Parallelization) as p:
        p.map(parse,listeLiens)
    
    #print duration of scraping
    deltaTime = datetime.utcnow() - startTime
    print('--> Processus executé en ',deltaTime)
    
if __name__ == "__main__":
    main()