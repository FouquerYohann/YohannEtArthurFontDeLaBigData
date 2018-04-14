# Projet DAR: Spark
## Spark SQL et Spark ML

Veuillez trouver ci-joint notre code ainsi que un JAR : com.projet.spark-1.0-SNAPSHOT.jar


Il est possible de trouver sur kaggle: https://www.kaggle.com/c/zillow-prize-1/data
4 fichiers contenant les données:
- deux fichiers par années un fichier properties_[année].csv contenant les caractéristiques des maisons et un fichier train_[année].csv contenant la prévision ( ces deux fichier peuvent être join par la colonne parcelid)


Une cible du JAR permet de générer les fichiers traités prêt pour le machine learning ils générent les fichiers déjà présent dans le repo : NumericFilledWithMeanAndStatisticDistribution2016.csv et NumericFilledWithMeanAndStatisticDistribution2017.csv
Et lance la régression linéaire sur ces deux fichiers
(Attention!!! Lancé sur la données complète ce main nécessite une config de spark augmenté et au moins 10go de mémoire alloué )
```bash
spark-submit --driver-memory 10G --executor-memory 15G --class YohannEtArthurFontDuDataScience.JustOne --master local /your/path/target/com.projet.spark-1.0-SNAPSHOT.jar /your/path/data/properties_2017.csv /your/path/data/train_2017.csv /your/path/data/properties_2016.csv /your/path/data/train_2016.csv
```

Une deuxième cible du main permet de lancer la regression linéaire directement sur des données déjà traité (comme les fichiers NumericFilledWithMeanAndStatisticDistribution201[6-7].csv)

```bash
spark-submit --class YohannEtArthurFontDuDataScience.LoadCsv --master local com.projet.spark-1.0-SNAPSHOT.jar [trainingDataSet].csv [testDataSet].csv
```
