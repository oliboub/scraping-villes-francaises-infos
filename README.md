# scraping-villes-francaises-infos
Scraping des informations des villes francaises (source https://www.linternaute.com/ville).
Ces scripts ont été réalisés pour adapter les scripts fournis dans la formation Udemy  'coder un dashboard' de RachidJ.

Les scripts du cours ont été actualisés pour prendre les infos en 2023.
je n'ai pas été regarder ceux mis à jour par Rachid sur le site Discord, le but étant d'apprendre par moi même.

Ils ne sont pas tous similaires, en terme de fonctions ou d'optimisations, car à la fin, je saturais, donc j'en ai optimisé certains.
je pense que Le plus structuré est celui des elections européennes.

J'ai ajouté un script pour vérifier les fichiers résultats: VerifieDuplication.py

Ces scripts sont écrits pour linux, fonctionne sous ubuntu 23 avec python 3.11 et un environnement virtuel.
A Savoir, c'est qu'apparement la parallelization en mode pool ne fonctionne pas sous windows. je n'ai pas été plus loin pour trouver une solution windows..
je vous laisse chercher.

La liste des librairies nécessaires:
- bs4
- requests
- pandas
- ipykernel (pour executer dans vs code en mode interactif)

Les scripts sont dans le répertoire <span style="color:orange">./scraping</span>
J'ai mis les fichiers résultats dans <span style="color:orange">./dataset</span>

Note: pour que les scripts fonctionnent, il faut etre positionné dans le répertoire ./scraping, car les fichiers dataset sont recherchés dans le répertoire <span style="color:orange">"../dataset"</span>

Le fichier  <span style="color:green">liensVilles.csv</span> est renommé <span style="color:green">villesTotal.csv</span>, car c'est plus simple quand je fais des tests avec <span style="color:green">villes1.csv</span>, <span style="color:green">villes5.csv</span> ou <span style="color:green">villes100.csv</span>