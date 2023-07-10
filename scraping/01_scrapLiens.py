import requests
from bs4 import BeautifulSoup as bs
import pandas as pd
from pandas import DataFrame
import csv

colonnes = ['ville','lien']
dico = {}
dico['ville'] = ''
dico['lien'] = ''

#Création de notre tableau
tableau = DataFrame(columns=colonnes)
tableau.to_csv('../dataset/liensVilles.csv',index=False)



lien = 'https://www.linternaute.com/ville/index/villes?page='


with open('../dataset/liensVilles.csv','a',encoding='utf-8') as csvfile:
    writer = csv.DictWriter(csvfile,fieldnames = colonnes,lineterminator='\n')
    
    for numeroPage in range(1,701):
        print(numeroPage)
        url = 'https://www.linternaute.com/ville/index/villes?page=' + str(numeroPage)
        req = requests.get(url)

        contenu=req.content
        soup = bs(contenu,"html.parser")
        tousLesLiens=soup.findAll('a')

#   print(len(tousLesLiens))

        for lien in tousLesLiens:
            if '/ville-' in lien['href']:
#               print(lien.text,'\t', lien['href'])
                dico['lien'] = lien['href']
                dico['ville'] = lien.text
    #           print(dico)
                writer.writerow(dico)      
