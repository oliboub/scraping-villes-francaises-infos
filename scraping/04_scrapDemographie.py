import requests
from multiprocessing import Pool
from bs4 import BeautifulSoup as bs
import pandas as pd
from pandas import DataFrame
import csv
from pprint import pprint
import os
import time
import json
from datetime import datetime
import sys

if sys.platform in ('win32', 'msys', 'cygwin'):
    print("Script works only on *NIX type operating systems.")
    sys.exit(1)


## diff: compare deux listes et retournce une liste avec la différence
def diff(list1, list2):
    return list(set(list1).symmetric_difference(set(list2))) # set retire les doublons


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

#Si la table est ville, on rempli avec 'nc'
def nc_table(ville, lien):
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

    #print(diconc)  
    with open(targetFile,'a',encoding='utf-8') as csvfile:
        writer=csv.DictWriter(csvfile,fieldnames=colonnes,lineterminator='\n')
        writer.writerow(diconc)

    return 

def parse(lien):

    def search_values(search, titre, dico,titro2 = ''):
        js_script = div.find('script').string
        json_data = json.loads(js_script)
        annees =  json_data['xAxis']['categories']
        donnees = json_data['series'][0]['data']
        a=1

        try:
            if json_data['series'][0] and json_data['series'][1]:
                donnees2=json_data['series'][1]['data']
                a=2
        except:
            a=1    
                                                
        for annee, donnee in zip(annees, donnees):
            #print(titre+ " (" +str(annee) + ")",annee,donnee)
            try:
                dico[titre+ " (" +str(annee) + ")"] = float(donnee)
            except:
                dico[titre+ " (" +str(annee) + ")"] = ''
        #print('a',a)

        if a ==2:
            #print('----------------------')
            #print(titro2)
            for annee, donnee2 in zip(annees, donnees2):
                #print(annee, donnee2)
                #print(titro2+ " (" +str(annee) + ")")
                try:
                    dico[titro2+ " (" +str(annee) + ")"] = float(donnee2)
                except:
                    dico[titro2+ " (" +str(annee) + ")"] = ''
                #print(dico[titro2+ " (" +str(annee) + ")"])
        #print(dico)
        return dico
    
    #print('--> start:',lien)
    # initialise le dictionnaire pour chaque scrap
    dico = {
    **{i : '' for i in listeCles},
    **{"nbre habitants (" + str(a) + ")" : '' for a in range(2006,2020)},
    **{"nbre naissances ("+str(a) + ")" : '' for a in range(1999,2020)},
    **{"nbre décès ("+str(a) + ")" : '' for a in range(1999,2020)},
    **{"nbre étrangers ("+str(a) + ")" : '' for a in range(2006,2019)},
    **{"nbre immigrés ("+str(a) + ")" : '' for a in range(2006,2019)},
    }
    req = requests.get(lien+'/'+categorie)
    time.sleep(2)
    start_func_time = datetime.utcnow()
    
    if req.status_code == 200:
        dico = {
            **{i : '' for i in listeCles},
            **{"nbre habitants (" + str(a) + ")" : '' for a in range(2006,2020)},
            **{"nbre naissances ("+str(a) + ")" : '' for a in range(1999,2020)},
            **{"nbre décès ("+str(a) + ")" : '' for a in range(1999,2020)},
            **{"nbre étrangers ("+str(a) + ")" : '' for a in range(2006,2019)},
            **{"nbre immigrés ("+str(a) + ")" : '' for a in range(2006,2019)},
            }
   
        contenu=req.content
        soup = bs(contenu,"html.parser")
                    
        dico['lien']=lien
        dico['ville'] = tableauLiens[tableauLiens['lien'] == lien]['ville'].iloc[0]
        
        tables=soup.findAll('table',class_ = 'odTable odTableAuto')
        if len(tables) != 0:
            #print("200 - pid:",os.getpid(),'\tlien:',lien)
            for i in range(len(tables)):
                cle ='nc'
                valeur='nc' 
                #print('\n------------- table ',i,'--------------')
                #print('cle','\t\t\tvaleur')
                infos = tables[i].findAll('tr')

                for info in infos[1:]:
                    cle = info.findAll('td')[0].text.split('(')[0].strip().replace(',',' -') # je remplace la ',' par un ' -' pour des compativilités avec certains lecteurs csv
                    valeur = info.findAll('td')[1].text
                    #print('valeur',valeur)
                    #cle = cle.split('(')[0].strip()
                    if not valeur:
                        valeur='nc'
                    else:
                        try:
                            valeur = float(valeur.split('h')[0].replace('%','').strip().replace(',','.').replace('\xa0',''))
                        except:
                            valeur = 'nc'
                    
                    #print('cle:',cle, '\tvaleur:',valeur)
                    dico[cle] = valeur
                
            #print(dico)
            divs=soup.findAll('div', class_='section-wrapper')
            #print(len(divs))
            
            #print('\n--------- First dico\n',dico)
            for div in divs:
                titre_h2= div.find('h2')
                
                #print(titre_h2)
                
                #--------------------------
                okidoki=0
                a=1
                search= "Combien y a-t-il d'habitants"
                titre ="nbre habitants"
                annees=[]
                donnees = []
                titre_span=div.find('span')
                
                if titre_span == None and titre_h2 != None and search in titre_h2.text:
                    okidoki=1
                        
                elif titre_span != None and titre_h2 != None and search in titre_span.text:
                    okidoki=1

                #print('okidoki',okidoki,'\ntitre_h2',titre_h2,'\ttitre_span:', titre_span)
                if okidoki == 1:
                    #print(search)
                    dico = search_values(search,titre,dico)
                #print('dico de retour',dico)
                            
                #--------------------
                okidoki=0
                a=1
                search= "Nombre d'étrangers"
                titre ="nbre étrangers"
                annees=[]
                donnees = []
                titre_span=div.find('span')
                #print('titre_h2',titre_h2,'\ttitre_span:', titre_span)
                if titre_span == None and titre_h2 != None and search in titre_h2.text:
                    okidoki=1
                        
                elif titre_span != None and titre_h2 != None and search in titre_span.text:
                    okidoki=1

                if okidoki == 1:
                    dico = search_values(search,titre,dico)
                
                #-----------------------
                okidoki=0
                a=1
                search= "Naissances et décès"
                titre ="nbre naissances"
                titre2 ="nbre décès"
                annees=[]
                donnees = []
                titre_span=div.find('span')
                #print('titre_h2',titre_h2,'\ttitre_span:', titre_span)
                if titre_span == None and titre_h2 != None and search in titre_h2.text:
                    okidoki=1
                        
                elif titre_span != None and titre_h2 != None and search in titre_span.text:
                    okidoki=1

                if okidoki == 1:
                    dico = search_values(search,titre,dico,titre2)     
                    
                #--------------------
                okidoki=0
                a=1
                search= "Nombre d'immigrés"
                titre = "nbre immigrés"
                annees=[]
                donnees = []
                titre_span=div.find('span')
                #print('titre_h2',titre_h2,'\ttitre_span:', titre_span)
                if titre_span == None and titre_h2 != None and search in titre_h2.text:
                    okidoki=1
                        
                elif titre_span != None and titre_h2 != None and search in titre_span.text:
                    okidoki=1

                if okidoki == 1:
                    dico = search_values(search,titre,dico)
                
            #print('---------------------------------\nDico finalisé:\n',dico)
            
            # mettre nc quand vide
            for i in dico:
                #print(i,dico[i])
                if  not dico[i] and dico[i] != 0:
                #    print('pas glop')
                    dico[i] = 'nc'
                #    print(dico[i])
            
            #print('---------------------------------\nDico finalisé:')
            #for i in dico:
            #    print(i,'\t',dico[i])
            with open("../dataset/demographie.csv",'a',encoding='utf-8') as csvfile:
                writer=csv.DictWriter(csvfile,fieldnames=colonnes,lineterminator='\n')
                writer.writerow(dico)
                delta_func_time = datetime.utcnow() - start_func_time
                print("200 - pid:",os.getpid(),'\tlien:',lien,'executé en \t',delta_func_time) 
            #print('-->', lien, ' executé en ',delta_func_time) 
        
        else:
            print("200 - pid:",os.getpid(),'\tlien:',lien,"\033[93m\033[1m(nc)\033[0m")
            nc_table(dico['ville'],dico['lien'])

               
    elif req.status_code ==  403:
        print(req.status_code," ",lien,'pid:',os.getpid())
        print("parallelizatrion", Parallelization,' is too high, or timesleep', timeSleep, ' is too low')
        print("Wait few minutes before relaunching, like site have probably blocked temporarly your internet adress..")
        sys.exit(1)
        
    elif req.status_code == 404:
        print("200 - pid:",os.getpid(),'\tlien:',lien,"\033[91m\033[1m(page not found)\033[0m")
        dico['lien'] = lien
        dico['ville'] = tableauLiens[tableauLiens['lien'] == lien1]['ville'].iloc[0]
        nc_table(dico['ville'],dico['lien'] )

    else:
        print(req.status_code, 'code à voir')                 

def main():
    ##############################
    # #Variables
    global startTime
    startTime = datetime.utcnow()
    
    global listeCles
    listeCles = ['ville','lien','Population', 'Densité de population', 'Nombre de ménages', 'Habitants par ménage',
            'Nombre de familles', 'Naissances', 'Décès', 'Solde naturel', 'Hommes', 'Femmes',
            'Moins de 15 ans', '15 - 29 ans', '30 - 44 ans', '45 - 59 ans', '60 - 74 ans', '75 ans et plus',
            'Agriculteurs exploitants', "Artisans - commerçants - chefs d'entreprise", 'Cadres et professions intellectuelles supérieures',
            'Professions intermédiaires', 'Employés', 'Ouvriers', 'Familles monoparentales', 'Couples sans enfant',
            'Couples avec enfant', 'Familles sans enfant', 'Familles avec un enfant', 'Familles avec deux enfants', 'Familles avec trois enfants',
            'Familles avec quatre enfants ou plus', 'Personnes célibataires', 'Personnes mariées', 'Personnes divorcées',
            'Personnes veuves', 'Personnes en concubinage', 'Personnes pacsées','15-24 ans étrangers',
            'Population étrangère', '55 ans et plus étrangers', 'Moins de 15 ans étrangers', 'Femmes étrangères',
            '25-54 ans étrangers', 'Hommes étrangers', 'Population immigrée', 'Hommes immigrés',
            'Femmes immigrées', 'Moins de 15 ans immigrés', '15-24 ans immigrés', '25-54 ans immigrés',
            '55 ans et plus immigrés']

    #Création du dico avec les clés mais aussi les valeurs par années de données recherchées dans la page démographie
    global dico
    dico = {
        **{i : '' for i in listeCles},
        **{"nbre habitants (" + str(a) + ")" : '' for a in range(2006,2021)},
        **{"nbre naissances ("+str(a) + ")" : '' for a in range(1999,2021)},
        **{"nbre décès ("+str(a) + ")" : '' for a in range(1999,2021)},
        **{"nbre étrangers ("+str(a) + ")" : '' for a in range(2006,2020)},
        **{"nbre immigrés ("+str(a) + ")" : '' for a in range(2006,2020)},
        }        
    sorted(dico.items(), key=lambda t: t[0])
    
    global colonnes
    colonnes= list(dico.keys())
    
    sourceFile="../dataset/villesTotal.csv"

    global categorie
    categorie = 'demographie'    
    
    global targetFile
    targetFile="../dataset/"+categorie+".csv"
    
    global tableauLiens
    tableauLiens = pd.read_csv(sourceFile,encoding='utf-8')       

    global listeLiens
    listeLiens=[]
    listeLiens=check_remaining()
    #print(len(listeLiens))
    
    global timeSleep
    timeSleep =2
    
    global Parallelization
    Parallelization = 26
    print('Launch with parallelization set to:',Parallelization)
    ########

    ## Launch sraping
    with Pool (Parallelization) as p:
        p.map(parse,listeLiens)
    
    # duration of scraping
    deltaTime = datetime.utcnow() - startTime
    print('--> Processus executé en ',deltaTime)
    
if __name__ == "__main__":
    main()