import pandas as pd
import sys
import os

#test = 1 
#test = 2
test = 3

sourceFile = "../dataset/villesTotal.csv"

def search(targetFile):
    if len(tableauInfos) == 0:
        print("le fichier", targetFile, "est \033[94m\033[1mvide.\033[0m")
           
    else:
        aa =tableauInfos.duplicated().sum()
        if aa > 0:
            print("le fichier", targetFile, "\tcontient\033[93m\033[1m",len(tableauInfos)-1,"\033[0mligne(s) de données et contient\033[91m\033[1m",aa, "\033[0mligne(s) en double(s)")
            print("lignes en doubles:\n",tableauInfos[tableauInfos.duplicated()])
        else:    
            print("le fichier", targetFile, "\tcontient\033[93m\033[1m",len(tableauInfos)-1,"\033[0mligne(s) de données et contient\033[92m\033[1m",aa, "\033[0mligne en double")  
        


def checkdTypes(targetFIle):
        print('')

def main():
    targetList = ["../dataset/villesTotal.csv",
                  "../dataset/sante-social.csv",
                  "../dataset/infos.csv",
                  "../dataset/autos.csv",
                  "../dataset/emplois.csv",
                  "../dataset/chomage.csv",
                  "../dataset/chomage1.csv",
                  "../dataset/delinquance.csv",
                  "../dataset/entreprises.csv",
                  "../dataset/immobilier.csv",
                  "../dataset/education.csv",
                  "../dataset/salaires.csv",
                  "../dataset/elections-europeennes.csv",]

    os.system('clear')
    global tableauInfos
    if test == 1 or test == 3:
        for targetFile in targetList:
            if os.path.isfile(targetFile):
                tableauInfos = pd.read_csv(targetFile,low_memory=False,encoding='utf-8')
                search(targetFile)
            else:
                print("le fichier", targetFile, "\033[91m\033[1m\test abscent\033[0m")

    if test == 2 or test == 3:
        for targetFile in targetList:
            if os.path.isfile(targetFile):
                tableauInfos = pd.read_csv(targetFile,low_memory=False,encoding='utf-8')
                print("\033[93m\033[1m",targetFile,'\033[0m\n',tableauInfos.dtypes,'\n========================')
            else:
                print("le fichier", targetFile, "\033[91m\033[1m\test abscent\033[0m")        
    

if __name__ == "__main__":
    main()
