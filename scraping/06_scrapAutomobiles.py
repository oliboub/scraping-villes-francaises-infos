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
import json
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

# changement d'url car site différent
def accidents_url(url):
    urlSplit = url.split('/')
    urlVille = urlSplit[4]
    urlVilleCode = urlSplit[5]
    #print(urlVille)
    urlAccidents="https://www.linternaute.com/auto/accident/"+urlVille+"/"+ urlVilleCode
    return urlAccidents


## diff: compare deux listes et retournce une liste avec la différence
def diff(list1, list2):
    return list(set(list1).symmetric_difference(set(list2))) # set retire les doublons

def nc_ville(ville, lien):
    #print(ville,lien)
    diconc ={}
    for i in listeCles:
        #print(i)
        if i == 'ville':
            #print('ville',ville,i)
            diconc[i] = ville
        elif i == 'lien':
            diconc[i] = lien              
        else:
            diconc[i] = 'nc'
    
## check_remaining : traite les diférences de liens entre un fichier de résultats déjà collectés et l'ensemble des liens à collecter, retoutne la liste des liens restants à traiter
def check_remaining():
  
    if os.path.isfile(targetFile):
        tableauTarget = pd.read_csv(targetFile,encoding='utf-8')
        colonne1 = tableauTarget['lien']
        colonne2 = tableauLiens['lien']
        #print(len(colonne1),len(colonne2))
        listeLiens = diff(colonne1,colonne2)
        listeLiens.sort()

    else:
        # Creation de notre csv infos
        tableauTarget = DataFrame(columns=colonnes)
        tableauTarget.to_csv(targetFile,index=False)
        listeLiens = tableauLiens['lien']
    
    print("Qtt restant",len(listeLiens),"pid:",os.getpid())
    return(listeLiens) 

### Function to scrap links (liens)
def search_tables(dico1,soup,_class):
    tables=soup.findAll('table',class_ = _class)

    #print(len(tables))
    if len(tables) != 0:
        ok=0
        #print("200 - pid:",os.getpid(),'\tlien:')
        for i in range (len(tables)): # dépend du nombre de tables qu'il va trouver dans chaque ville, car ça peut fluctuer
            tousLesTr = tables[i].findAll('tr')
            
            for tr in tousLesTr[1:]: # affiche à partir de l'index 1 et evite le premier tr
                cle = tr.findAll('td')[0].text
                #print(cle)
                #valeur = tr.findAll('td')[1].text
                valeur1=tr.findAll('td')[1].text.replace('%','').replace(" ","").replace(' ','').split('(')[0].strip()
                try:
                    #valeur = tr.findAll('td')[1].text
                    #valeur = float(''.join(valeur.split()))
                    valeur = float(valeur1)
                    #valeur1 = float(tr.findAll('td')[1].text.replace('%','').replace(" ","").replace(' ','').split('(')[0].strip())
                    #print('valeur',valeur)
                                            
                except:
                    valeur = valeur1 # tr.findAll('td')[1].text.replace('%','').strip().replace(" ","").replace(' ','')
                    # print('valeur except',valeur)
                dico1[cle] = valeur
    else:
            ok=1

            #print(dico['ville'],dico['lien'] )
            #nc_ville(dico['ville'],dico['lien'] )
     
    return ok,dico1
     
def parse(lien):
    
    #lien1 = lien
    
    dico1 = {

        **{ i : '' for i in listeCles },
    }
    #dico1=dico
    #print('dico1 in parse',dico1)    
    ##############################################""""""""""    
    ##première page de recherche
    req = requests.get(lien+'/'+categorie)
    time.sleep(timeSleep)
    start_func_time = datetime.utcnow()

    if req.status_code == 200:

        contenu=req.content 
        soup = bs(contenu,"html.parser")
            
        dico1['lien'] = lien
        dico1['ville'] = tableauLiens[tableauLiens['lien'] == lien]['ville'].iloc[0]
        
        ok1,dico1=search_tables(dico1,soup,'odTable odTableAuto')
        #print('dico1 après 1er search',dico1)
        
               
    elif req.status_code ==  403:
        print(req.status_code," ",lien,'pid:',os.getpid())
        print("parallelizatrion", Parallelization,' is too high, or timesleep', timeSleep, ' is too low')
        print("Wait few minutes before relaunching, like site have probably blocked temporarly your internet adress..")
        sys.exit(1)
        
    elif req.status_code == 404:
        print("200 - pid:",os.getpid(),'\tlien:',lien,"\033[91m\033[1m(page not found)\033[0m")
        dico1['lien'] = lien
        dico1['ville'] = tableauLiens[tableauLiens['lien'] == lien]['ville'].iloc[0]
        nc_ville(dico1['ville'],dico1['lienAccidents'] )
    else:
        print(req.status_code, 'code à voir')

    #
    # print('first search:'dico)
    
    ################################################"
    # Deuxieme page de recherche
    lien1=accidents_url(lien)
    req = requests.get(lien1)
    time.sleep(timeSleep)
    start_func_time = datetime.utcnow()

    if req.status_code == 200:

        contenu=req.content
        soup = bs(contenu,"html.parser")
            
        dico1['lienAccidents'] = lien1
        #dico['ville'] = tableauLiens[tableauLiens['lien1'] == lien1]['ville'].iloc[0]
     
        #print('##########################################')
        #print('dico avant 2eme search',dico)
        ok2,dico1=search_tables(dico1,soup,'odTable odTableAuto')
        #print('dico après 2eme search',dico)
        
        #--------------------
        #Save file
        
        # mettre nc quand vide
        for i in dico1:
            #print(i,dico1[i])
            if  not dico1[i] and dico1[i] != 0:
            #    print('pas glop')
                dico1[i] = 'nc'
            #    print(dico[i])
                
        with open(targetFile,'a',encoding='utf-8') as csvfile:
            writer=csv.DictWriter(csvfile,fieldnames=colonnes,lineterminator='\n')
            writer.writerow(dico1)
            delta_func_time = datetime.utcnow() - start_func_time
            if ok1 == 0 and ok2 == 0:
                print("200 - pid:",os.getpid(),'\tlien:',lien,'executé en \t',delta_func_time) 
            else: 
                print("200 - pid:",os.getpid(),'\tlien:',lien,"\033[93m\033[1m(nc)\033[0m")
        #     #print(dico)
        
        
            
    elif req.status_code ==  403:
        print(req.status_code," ",lien1,'pid:',os.getpid())
        print("parallelizatrion", Parallelization,' is too high, or timesleep', timeSleep, ' is too low')
        print("Wait few minutes before relaunching, like site have probably blocked temporarly your internet adress..")
        sys.exit(1)
        
    elif req.status_code == 404:
        print("200 - pid:",os.getpid(),'\tlien:',lien,"\033[91m\033[1m(page not found)\033[0m")
        dico1['lienAccidents'] = lien1
        dico1['ville'] = tableauLiens[tableauLiens['lien'] == lien1]['ville'].iloc[0]
        nc_ville(dico1['ville'],dico1['lienAccidents'] )

    else:
        print(req.status_code, 'code à voir')
    
        
def main():
    ##############################
    # #Variables
    global startTime
    startTime= datetime.utcnow()
      
    global listeCles
    listeCles = ['ville', 'lien','lienAccidents', 'Ménages sans voiture', 'Ménages avec une voiture', 
                 'Ménages avec deux voitures ou plus', 'Ménages avec place(s) de stationnement',
                 "Nombre total d'accidents", 'Nombre de personnes tuées',
                 'Nombre de personnes indemnes', 'Nombre de personnes blessées', 
                 ' - dont blessés graves', ' - dont blessés légers']
        
    global dico
    dico = {

        **{ i : '' for i in listeCles },
    }
   
    sorted(dico.items(), key=lambda t: t[0])
    
    global colonnes
    colonnes= list(dico.keys())
    
    sourceFile="../dataset/villesTotal.csv"

    global categorie
    categorie = 'auto'    
    
    global targetFile
    targetFile="../dataset/"+categorie+".csv"
    
    global tableauLiens
    tableauLiens = pd.read_csv(sourceFile,encoding='utf-8')       

    global listeLiens
    listeLiens=check_remaining()
    #print(len(listeLiens))
    
    global timeSleep
    timeSleep =2
    
    global Parallelization
    Parallelization = 20
    print('Launch with parallelization set to:',Parallelization)
    ##############################

    ## Launch sraping
    with Pool (Parallelization) as p:
        p.map(parse,listeLiens)
    
    # duration of scraping
    deltaTime = datetime.utcnow() - startTime
    print('--> Processus executé en ',deltaTime)
    
if __name__ == "__main__":
    main()