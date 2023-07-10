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

## diff: compare deux listes et retournce une liste avec la différence
def diff(list1, list2):
    return list(set(list1).symmetric_difference(set(list2))) # set retire les doublons


# changement d'url car site différent
def elections_url(url):
    urlSplit = url.split('/')
    urlVille = urlSplit[4]
    urlVilleCode = urlSplit[5]
    #print(urlVille)
    urlElectionsEU="https://election-europeenne.linternaute.com/resultats/"+urlVille+"/"+ urlVilleCode
    return urlElectionsEU

def nc_ville(ville, lien, urlElection):
    #print(ville,lien,urlElection)
    diconc ={}
    for i in listeCles:
        #print(i)
        if i == 'ville':
            #print('ville',ville,i)
            diconc[i] = ville
        elif i == 'lien':
            diconc[i] = lien              
        elif i == 'urlElection':
            diconc[i]=urlElection
        else:
            diconc[i] = 'nc'

    #print(diconc)  
    with open(targetFile,'a',encoding='utf-8') as csvfile:
        writer=csv.DictWriter(csvfile,fieldnames=colonnes,lineterminator='\n')
        writer.writerow(diconc)

    return 
    
    
## check_remaining : traite les diférences de liens entre un fichier de résultats déjà collectés et l'ensemble des liens à collecter, retoutne la liste des liens restants à traiter
def check_remaining():
  
    global tableauLiens
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

     
def parse(lien):
    
    lien1 = lien
    urlElection = elections_url(lien)
    
    #print('dico in parse',dico)    
        
    req = requests.get(urlElection)
    time.sleep(timeSleep)
    start_func_time = datetime.utcnow()
    
    if req.status_code == 200:

        #with open(targetFile,'a',encoding='utf-8') as csvfile:
            #writer=csv.DictWriter(csvfile,fieldnames=colonnes,lineterminator='\n')
        contenu=req.content
        soup = bs(contenu,"html.parser")
            
        dico['lien'] = lien
        dico['urlElection'] = urlElection
        dico['ville'] = tableauLiens[tableauLiens['lien'] == lien1]['ville'].iloc[0]
        
        tables=soup.findAll('table',class_ = 'od_table--grid--col2')

        #print(len(tables))
        if len(tables) != 0:
            #print("200 - pid:",os.getpid(),'\tlien:',urlElection)
            for i in range (len(tables)): # dépend du nombre de tables qu'il va trouver dans chaque ville, car ça peut fluctuer
                tousLesTr = tables[i].findAll('tr')
                dico['lien'] = lien 
                for tr in tousLesTr[1:]: # affiche à partir de l'index 1 et evite le premier tr
                    cle = tr.findAll('td')[0].text
                    #print(cle)
                    #valeur = tr.findAll('td')[1].text
                    #print(valeur)
                    try:
                        #valeur = tr.findAll('td')[1].text
                        #valeur = float(''.join(valeur.split()))
                        valeur = float(tr.findAll('td')[1].text.replace('%','').strip().replace(" ",""))
                        #print(valeur)
                                                
                    except:
                        valeur = tr.findAll('td')[1].text.replace('%','').strip().replace(" ","")
                    dico[cle] = valeur

            dico1 = dico
            
            tetesListes = soup.findAll('table',class_ = "od_table--grid--col3 elections_table--candidats-with-pic")
            #print('len tetesListes',len(tetesListes))

            for i in range (len(tetesListes)):
                tousLesTr1 = tetesListes[i].findAll('tr')
                #print(tousLesTr1)

            for tr1 in tousLesTr1[1:]: # affiche à partir de l'index 1 et evite le premier tr
                txt0 = tr1.findAll('td')[0].text.split('\n')[2]
                txt1 = tr1.findAll('td')[1].text.replace('%','').strip()
                dico[txt0] = txt1
            
            dico1 =dico
            #print(dico1)    
            
            #--------------------
            # Save file
            
            with open(targetFile,'a',encoding='utf-8') as csvfile:
                writer=csv.DictWriter(csvfile,fieldnames=colonnes,lineterminator='\n')
                writer.writerow(dico1)
                delta_func_time = datetime.utcnow() - start_func_time
                print("200 - pid:",os.getpid(),'\tlien:',urlElection,'executé en \t',delta_func_time) 
                #print(dico1)
        else:
            print("200 - pid:",os.getpid(),'\tlien:',urlElection,"\033[93m\033[1m(nc)\033[0m")
            nc_ville(dico['ville'],dico['lien'], dico['urlElection'] )

               
    elif req.status_code ==  403:
        print(req.status_code," ",lien,'pid:',os.getpid())
        print("parallelizatrion", Parallelization,' is too high, or timesleep', timeSleep, ' is too low')
        print("Wait few minutes before relaunching, like site have probably blocked temporarly your internet adress..")
        sys.exit(1)
        
    elif req.status_code == 404:
        print("200 - pid:",os.getpid(),'\tlien:',urlElection,"\033[91m\033[1m(page not found)\033[0m")
        dico['lien'] = lien
        dico['urlElection'] = urlElection
        dico['ville'] = tableauLiens[tableauLiens['lien'] == lien1]['ville'].iloc[0]
        nc_ville(dico['ville'],dico['lien'], dico['urlElection'] )

        
    else:
        print(req.status_code, 'code à voir')


        
def main():
    ##############################
    # #Variables
    global startTime
    startTime= datetime.utcnow()
      
    global listeCles
    listeCles = ['ville','lien','urlElection','Taux de participation', "Taux d'abstention", 'Votes blancs (en pourcentage des votes exprimés)',
                 'Votes nuls (en pourcentage des votes exprimés)', 'Nombre de votants',
                 'Nathalie LOISEAU', 'Yannick JADOT', 'Jordan BARDELLA', 'Raphaël GLUCKSMANN', 'Manon AUBRY', 'François-Xavier BELLAMY',
                 'Benoît HAMON', 'Dominique BOURG', 'Nicolas DUPONT-AIGNAN', 'Jean-Christophe LAGARDE', 'Ian BROSSAT', 'Hélène THOUY', 'François ASSELINEAU',
                 'Nathalie ARTHAUD', 'Francis LALANNE', 'Florie MARIE', 'Florian PHILIPPOT', 'Olivier BIDOU', 'Nagib AZERGUI', 'Yves GERNIGON', 'Pierre DIEUMEGARD',
                 'Sophie CAILLAUD', 'Gilles HELGEN', 'Thérèse DELFEL', 'Vincent VAUCLIN', 'Cathy Denise Ginette CORBET', 'Nathalie TOMASINI', 'Audric ALEXANDRE',
                 'Christophe CHALENÇON', 'Hamada TRAORÉ', 'Robert DE PREVOISIN', 'Renaud CAMUS', 'Christian Luc PERSON', 'Antonio SANCHEZ']
        
    global dico
    dico = {

        **{ i : '' for i in listeCles },
    }
   
    sorted(dico.items(), key=lambda t: t[0])
    
    global colonnes
    colonnes= list(dico.keys())
    
    sourceFile="../dataset/villesTotal.csv"

    global categorie
    categorie = 'elections-europeennes'    
    
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
    Parallelization = 25
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