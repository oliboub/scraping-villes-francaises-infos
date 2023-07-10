# scraping-villes-francaises-infos
Scraping des informatiosn des villes francaises (source https://www.linternaute.com/ville) basée sur une formation Udemy  'coder un dashboard'

Les scripts du cours ont été actualisés pour prendre les infos en 2023

Ils ne sont pas tous similaires, en terme de fonctions ou d'optimisations, car à la fin, je saturais, donc j'ai optimisé certains.
le plus structuré, je pense, est celui des elecitiosn européennes.

J'ai ajouté un script pour vérifier les fichiers résultats: VerifieDuplication.py

Ce script est écrit pour linux, fonctionne sous ubuntu 23 avec python 3.11 et un envioronnement virtuel.
A Savori, c'est qu'apparement la paralleliaztion en mode pool, ne fonctionne aps sous windows. je n'ai pas été plus loin pour trouver une solution windows..
je vous laisse chercher.

La liste des librairies nécessaires:<br>
- bs4
- requests
- pandas
- ipykernel (pour executer dans vs code en mode interactif

Les scripts sont dans le répertoire <span style="color:orange">/scraping</span><br>
J'ai mis les fichiers résutlats dans <span style="color:orange">./dataset</span>
