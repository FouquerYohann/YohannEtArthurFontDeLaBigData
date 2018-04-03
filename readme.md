#Rapport projet DAR
##Spark SQL et Spark ML
Comparaison des résultats après différentes modifications sur les données :
 ##### - Remplissage des valeurs naîves
 Remplissage des colonnes associées à un "flag" et des valeurs absentes par 0.
 ##### - Remplissage des espaces vide par la médiane/moyenne de leur colonne
 ##### - Remplissage des espaces vide grâce a un arbre de décision entrainé sur les autres colonnes
 ##### - Ajout de nouvelles colonnes de feature engineering