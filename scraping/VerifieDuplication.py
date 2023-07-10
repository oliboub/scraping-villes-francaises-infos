import pandas as pd
import sys
import os


sourceFile = "../dataset/villesTotal.csv"

def search(targetFile):
    if os.path.isfile(targetFile):
        tableauInfos = pd.read_csv(targetFile,low_memory=False,encoding='utf-8')
    
        

        if len(tableauInfos) == 0:
           print("le fichier", targetFile, "est \033[94m\033[1mvide.\033[0m")
           
        
        else:
            aa =tableauInfos.duplicated().sum()
            if aa > 0:
                print("le fichier", targetFile, "contient\033[93m\033[1m",len(tableauInfos)-1,"\033[0mligne(s) et contient\033[91m\033[1m",aa, "\033[0mligne(s) en double(s)")
                print("lignes en doubles:\n",tableauInfos[tableauInfos.duplicated()])
            else:    
                print("le fichier", targetFile, "contient\033[93m\033[1m",len(tableauInfos)-1,"\033[0mligne(s) et contient\033[92m\033[1m",aa, "\033[0mligne en double")  
            
    else:
        print('pas de fichier', targetFile)
        sys.exit(1)

def main():
    targetList = ["../dataset/villesTotal.csv",
                  "../dataset/sante-social.csv",
                  "../dataset/infos.csv",
                  "../dataset/auto.csv",
                  "../dataset/emplois.csv",
                  "../dataset/chomage.csv",
                  "../dataset/chomage1.csv",
                  "../dataset/delinquance.csv",
                  "../dataset/entreprises.csv",
                  "../dataset/immobilier.csv",
                  "../dataset/education.csv",
                  "../dataset/elections-europeennes.csv",]

   
    print('\n##################################')
    for targetFiles in targetList:
        search(targetFiles)
    print('##################################')

if __name__ == "__main__":
    main()
