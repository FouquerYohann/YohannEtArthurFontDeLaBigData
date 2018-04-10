#Rapport projet DAR
##Spark SQL et Spark ML

###Imputing des features manquante
  Les colonnes comportant des flags manquant sont en fait signe qu'il est faux. Remplacement des flags manquant par la valeur `false` dans les colonnes "fireplaceflag", "taxdelinquencyflag".
   
  De meme dans les colonnes "fireplacecnt", "poolsizesum", "taxdelinquencyyear", "garagetotalsqft", vu le nombre de valeurs manquante il s'agit en fait que manquante signifie qu'il n'y en pas et donc nous les avons remplis par `0`
  par contre la colonne "unitcnt" sera remplis de `1` car celle ci correspond au nombre de  ???
  
    
    
###Comparaison des résultats RMSE après différentes modifications sur les données : 
 ```scala
val predictionAndObservations = props
  .select("prediction", Array("logerror"): _*)
  .rdd
  .map(row => (row.getDouble(0), row.getDouble(1)))
val metrics = new RegressionMetrics(predictionAndObservations)
val rmse = metrics.rootMeanSquaredError
 ```
 ##### - Remplissage des valeurs naîves
 <p>Remplissage des colonnes associées à un "flag" et des valeurs absentes par 0.</p>
 <p>Résultat de l'apprentissage par régression linéaire avec les colonnes :
     "assessmentyear",
      "bathroomcnt",
      "bedroomcnt",
      "calculatedbathnbr",
      "calculatedfinishedsquarefeet",
      "fireplacecnt",
      "finishedsquarefeet12",
      "fullbathcnt",
      "garagecarcnt",
      "garagetotalsqft",
      "landtaxvaluedollarcnt",
      "latitude",
      "longitude",
      "lotsizesquarefeet",
      "numberofstories",
      "poolcnt",
      "poolsizesum",
      "roomcnt",
      "structuretaxvaluedollarcnt",
      "unitcnt",
      "taxamount",
      "taxdelinquencyyear",
      "taxvaluedollarcnt",
      "yearbuilt"</p>
  <p>Train_2016: 0.1583605719585012</p> 
  <p>Train_2017: 0.15472756334029183</p> 
 
 ##### - Remplissage des espaces vide par la médiane/moyenne de leur colonne
 Avec répartition statistique aléatoire des colonnes catégoriques :
  <p>Train_2016: 0.1583605719585012</p> 
  <p>Train_2017: 0.15472756334029183</p> 
  
 ##### - Remplissage des espaces vide grâce a un arbre de décision entrainé sur les autres colonnes
 ##### - Ajout de nouvelles colonnes de feature engineering
 Pour augmenter la précision du modèle il est également possible d'ajouter de nouvelles colonnes en utilisant les informations déja présentes.
 Nous avons eu plusieurs idées de feature engineering :
 - distance avec le centre commerciale le plus proche (utilisant des données de open street map)
 - distance avec la plage