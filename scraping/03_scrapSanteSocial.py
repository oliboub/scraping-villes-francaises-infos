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

#fonction qui va faire une difference entre liensVilles.csv et infos.csv
def diff(list1, list2):
    return list(set(list1).symmetric_difference(set(list2))) # set retire les doublons


### Function to check remaining links to process
def check_remaining():
    #source="../dataset/villes1.csv"
    
    global tableauLiens
    if os.path.isfile(targetFile):
        tableauSante = pd.read_csv(targetFile,encoding='utf-8')
        colonne1 = tableauSante['lien']
        colonne2 = tableauLiens['lien']
        listeLiens = diff(colonne1,colonne2)
        listeLiens.sort()
    
    else:
        # Creation de notre csv infos
        tableauSante = DataFrame(columns=colonnes)
        tableauSante.to_csv(targetFile,index=False)
        listeLiens = tableauLiens['lien']
    
    print("Qtt restant",len(listeLiens),"pid:",os.getpid())
    return(listeLiens) 

### Function to scrap links (liens)

     
def parse(lien):
    
    def search_values(search, titre, dico,titro2 = ''):
        #print(search,titre,dico,titro2)         
        js_script = div.find('script').string
        #pprint(js_script)
        json_data=json.loads(js_script)
        annees =  json_data['xAxis']['categories']
        donnees = json_data['series'][0]['data']
        #print(titre)
        #pprint(json_data['series'])
        try:
            a=2
            titre1= json_data['series'][0]['name']
            donnees2=json_data['series'][1]['data']
            titre2= json_data['series'][1]['name']
            
        except:
            titre1= search
            a=1
            
        #print('----------------------')
        #print(titre1)
        for annee, donnee in zip(annees, donnees):
            #print(annee, donnee)
            #print(titre+ " (" +str(annee) + ")")
            try:
                dico[titre+ " (" +str(annee) + ")"] = float(donnee)
            except:
                dico[titre+ " (" +str(annee) + ")"] = donnee
            #print(dico[titre+ " (" +str(annee) + ")"])
            
        if a ==2:
            #print('----------------------')
            #print(titre2)
            for annee, donnee2 in zip(annees, donnees2):
                #print(annee, donnee2)
                #print(titro2+ " (" +str(annee) + ")")
                try:
                    dico[titro2+ " (" +str(annee) + ")"] = float(donnee2)
                except:
                    dico[titro2+ " (" +str(annee) + ")"] = donnee2
                #print(dico[titro2+ " (" +str(annee) + ")"])
        #print(dico)
        return dico
    
    
    
    
    #print('dico in parse',dico)    
        
    req = requests.get(lien+'/'+categorie)
    time.sleep(timeSleep)
    
    if req.status_code == 200:
        print("200 - pid:",os.getpid(),'\tlien:',lien+"/"+categorie)

        #with open(targetFile,'a',encoding='utf-8') as csvfile:
            #writer=csv.DictWriter(csvfile,fieldnames=colonnes,lineterminator='\n')
        contenu=req.content
        soup = bs(contenu,"html.parser")
            
        dico['lien'] = lien
        dico['ville'] = tableauLiens[tableauLiens['lien'] == lien]['ville'].iloc[0]

        tables=soup.findAll('table',class_ = 'odTable odTableAuto')


        for i in range (len(tables)): # dépend du nombre de tables qu'il va trouver dans chaque ville, car ça peut fluctuer
            tousLesTr = tables[i].findAll('tr')
            dico['lien'] = lien 
            for tr in tousLesTr[1:]: # affiche à partir de l'index 1 et evite le premier tr
                cle = tr.findAll('td')[0].text
                #valeur = tr.findAll('td')[1].text.replace(" ","")
                try:
                    #valeur = tr.findAll('td')[1].text
                    #valeur = float(''.join(valeur.split()))
                    valeur = float(tr.findAll('td')[1].text.replace(" ",""))
                except:
                    valeur = tr.findAll('td')[1].text
                dico[cle] = valeur
           #pprint(dico['lien'])

        dico1 = dico
        #print(dico1)
    else:
        print(req.status_code," ",lien,'pid:',os.getpid())
        print("parallelizatrion", Parallelization,' is too high, or timesleep', timeSleep, ' is too low')
        print("Wait few minutes before relaunching, like site have probably blocked temporarly your internet adress..")
        sys.exit(1)
    
    divs=soup.findAll('div', class_='section-wrapper')
    #print(len(divs))
    
    #print('\n--------- First dico\n',dico)
    for div in divs:
        titre_h2= div.find('h2')
        #print(titre_h2)
        
        #--------------------------
        # nbre allocataires CAF
        okidoki=0
        a=1
        search= "Combien d'allocataires CAF"
        titre ="nbre allocataires CAF"
        annees=[]
        donnees = []
        titre_span=div.find('span')
        
        #print(titre_span)
        if titre_span == None and titre_h2 != None and search in titre_h2.text:
            okidoki=1
                
        elif titre_span != None and titre_h2 != None and search in titre_span.text:
            okidoki=1

        #print('okidoki',okidoki,'\ntitre_h2',titre_h2,'\ttitre_span:', titre_span)
        if okidoki == 1:
            #print(okidoki, search)
            dico1 = search_values(search,titre,dico)
            #print('dico de retour',dico1)

        #--------------------------
        # nbre beneficiaires rsa
        okidoki=0
        a=1
        search= "Combien de bénéficiaires du RSA"
        titre ="nbre beneficiaires rsa"
        annees=[]
        donnees = []
        titre_span=div.find('span')
        
        #print(titre_span)
        if titre_span == None and titre_h2 != None and search in titre_h2.text:
            okidoki=1
                
        elif titre_span != None and titre_h2 != None and search in titre_span.text:
            okidoki=1

        #print('okidoki',okidoki,'\ntitre_h2',titre_h2,'\ttitre_span:', titre_span)
        if okidoki == 1:
            #print(okidoki, search)
            dico1 = search_values(search,titre,dico)
            #print('dico de retour',dico1)
 
        #--------------------------
        # nbre beneficiaires apl
        okidoki=0
        a=1
        search= "Nombre de bénéficiaires de l'aide au logement"
        titre ="nbre beneficiaires apl"
        annees=[]
        donnees = []
        titre_span=div.find('span')
        
        #print(titre_span)
        if titre_span == None and titre_h2 != None and search in titre_h2.text:
            okidoki=1
                
        elif titre_span != None and titre_h2 != None and search in titre_span.text:
            okidoki=1

        #print('okidoki',okidoki,'\ntitre_h2',titre_h2,'\ttitre_span:', titre_span)
        if okidoki == 1:
            #print(okidoki, search)
            dico1 = search_values(search,titre,dico)
            #print('dico de retour',dico1)         

#       --------------------------
        # nbre beneficiaires alloc familiales
        okidoki=0
        a=1
        search= "Nombre de bénéficiaires des allocations familiales"
        titre ="nbre beneficiaires alloc familiales"
        annees=[]
        donnees = []
        titre_span=div.find('span')
        
        #print(titre_span)
        if titre_span == None and titre_h2 != None and search in titre_h2.text:
            okidoki=1
                
        elif titre_span != None and titre_h2 != None and search in titre_span.text:
            okidoki=1

        #print('okidoki',okidoki,'\ntitre_h2',titre_h2,'\ttitre_span:', titre_span)
        if okidoki == 1:
            #print(okidoki, search)
            dico1 = search_values(search,titre,dico)
            #print('dico de retour',dico1)         

        #--------------------------
        # nbre médecins
        okidoki=0
        a=1
        search= "Combien de médecins"
        titre ="nbre médecins"
        annees=[]
        donnees = []
        titre_span=div.find('span')
        
        #print(titre_span)
        if titre_span == None and titre_h2 != None and search in titre_h2.text:
            okidoki=1
                
        elif titre_span != None and titre_h2 != None and search in titre_span.text:
            okidoki=1

        #print('okidoki',okidoki,'\ntitre_h2',titre_h2,'\ttitre_span:', titre_span)
        if okidoki == 1:
            #print(okidoki, search)
            dico1 = search_values(search,titre,dico)
            #print('dico de retour',dico1)      
      
    #--------------------
    # Save file
    
    with open(targetFile,'a',encoding='utf-8') as csvfile:
        writer=csv.DictWriter(csvfile,fieldnames=colonnes,lineterminator='\n')
        writer.writerow(dico1)
    
        
def main():
    ##############################
    # #Variables
    global startTime
    startTime= datetime.utcnow()
      
    listeCles = ["ville","lien","Allocataires CAF","Bénéficiaires du RSA"," - bénéficiaires du RSA majoré",
                " - bénéficiaires du RSA socle","Bénéficiaires de l'aide au logement"," - bénéficiaires de l'APL (aide personnalisée au logement)",
                " - bénéficiaires de l'ALF (allocation de logement à caractère familial)"," - bénéficiaires de l'ALS (allocation de logement à caractère social)",
                " - bénéficiaires de l'Allocation pour une location immobilière"," - bénéficiaires de l'Allocation pour un achat immobilier","Bénéficiaires des allocations familiales",
                " - bénéficiaires du complément familial"," - bénéficiaires de l'allocation de soutien familial"," - bénéficiaires de l'allocation de rentrée scolaire","Bénéficiaires de la PAJE",
                " - bénéficiaires de l'allocation de base"," - bénéficiaires du complément mode de garde pour une assistante maternelle",
                " - bénéficiaires du complément de libre choix d'activité (CLCA ou COLCA)",
                " - bénéficiaires de la prime naissance ou adoption","Médecins généralistes","Masseurs-kinésithérapeutes","Dentistes","Infirmiers",
                "Spécialistes ORL","Ophtalmologistes","Dermatologues","Sage-femmes","Pédiatres","Gynécologues","Pharmacies","Urgences","Ambulances",
                "Etablissements de santé de court séjour","Etablissements de santé de moyen séjour","Etablissements de santé de long séjour",
                "Etablissement d'accueil du jeune enfant","Maisons de retraite","Etablissements pour enfants handicapés","Etablissements pour adultes handicapés"]
    
    global dico
    dico = {
        **{ i : '' for i in listeCles },
        **{"nbre allocataires CAF (" + str(a) + ")" : '' for a in range(2009,2022)},
        **{"nbre beneficiaires rsa (" + str(a) + ")" : '' for a in range(2009,2022)},
        **{"nbre beneficiaires apl (" + str(a) + ")" : '' for a in range(2009,2022)},
        **{"nbre beneficiaires alloc familiales (" + str(a) + ")" : '' for a in range(2009,2022)},
        **{"nbre médecins (" + str(a) + ")" : '' for a in range(2009,2022)},
    }
   
    sorted(dico.items(), key=lambda t: t[0])
    
    global colonnes
    colonnes= list(dico.keys())
    
    sourceFile="../dataset/villesTotal.csv"

    global categorie
    categorie = 'sante-social'    
    
    global targetFile
    targetFile="../dataset/"+categorie+".csv"
    
    global tableauLiens
    tableauLiens = pd.read_csv(sourceFile,encoding='utf-8')       

    listeLiens=check_remaining()
    
    global timeSleep
    timeSleep =2
    
    global Parallelization
    Parallelization = 30
    ##############################

    #Launch sraping
    with Pool (Parallelization) as p:
        p.map(parse,listeLiens)
    
    # duration of scraping
    deltaTime = datetime.utcnow() - startTime
    print('--> Processus executé en ',deltaTime)
    
if __name__ == "__main__":
    main()