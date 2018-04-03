#Rapport projet DAR
##Spark SQL et Spark ML
Comparaison des résultats RMSE après différentes modifications sur les données : 
 ```scala
val predictionAndObservations = props
  .select("prediction", Array("logerror"): _*)
  .rdd
  .map(row => (row.getDouble(0), row.getDouble(1)))
val metrics = new RegressionMetrics(predictionAndObservations)
val rmse = metrics.rootMeanSquaredError
 ```
 ##### - Remplissage des valeurs naîves
 Remplissage des colonnes associées à un "flag" et des valeurs absentes par 0.

 <p>Train_2016: 0.1583605719585012</p> 
 <p>Train_2017: 0.15472756334029183</p> 
 
 ##### - Remplissage des espaces vide par la médiane/moyenne de leur colonne
 Avec répartition statistique aléatoire des colonnes catégoriques :
  <p>Train_2016: 0.1583605719585012</p> 
  <p>Train_2017: 0.15472756334029183</p> 
  
 ##### - Remplissage des espaces vide grâce a un arbre de décision entrainé sur les autres colonnes
 ##### - Ajout de nouvelles colonnes de feature engineering