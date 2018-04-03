package fr.stl.dar

import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.ml.feature.{StringIndexer, OneHotEncoder, VectorAssembler}
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.ml.Pipeline
import org.apache.spark.mllib.evaluation.RegressionMetrics


object TfiRevenueEstimator {

  def readCsv(sparkSession: SparkSession, path: String): DataFrame = {
    sparkSession.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(path)
  }


  def runTrainingAndEstimation(data: DataFrame, labelCol: String, numFeatCols: Array[String],
    categFeatCols: Array[String] = Array()): DataFrame = {

    val lr = new LinearRegression().setLabelCol(labelCol)
    val assembler = new VectorAssembler()
    val pipeline = new Pipeline()

    if (categFeatCols.length == 0) {

      assembler
        .setInputCols(numFeatCols)
        .setOutputCol("features")
      pipeline.setStages(Array(assembler, lr))
  
    } else {
      
      var featureCols = numFeatCols
      val indexers = categFeatCols.map(c =>
        new StringIndexer().setInputCol(c).setOutputCol(s"${c}_idx")
      )
      val encoders = categFeatCols.map(c => {
        val outputCol = s"${c}_enc"
        featureCols = featureCols :+ outputCol
        new OneHotEncoder().setInputCol(s"${c}_idx").setOutputCol(outputCol)
      })
      assembler
        .setInputCols(featureCols)
        .setOutputCol("features")
      pipeline.setStages(indexers ++ encoders ++ Array(assembler, lr))
    }

    val Array(trainSet, testSet) = data
      .randomSplit(Array(0.9, 0.1), seed=12345)

    // Entrainement du modèle sur trainSet
    val modelLR = pipeline.fit(trainSet)

    // Prédiction sur testSet
    val predictions = modelLR.transform(testSet)
    predictions.select("prediction", labelCol).show()

    val predictionsAndObservations = predictions
      .select("prediction", labelCol)
      .rdd
      .map(row => (row.getDouble(0), row.getDouble(1)))
    val metrics = new RegressionMetrics(predictionsAndObservations)
    val rmse = metrics.rootMeanSquaredError
    println("RMSE: " + rmse)

    predictions
  }

  def main(arg: Array[String]): Unit = {
    val spark = SparkSession.builder
      .master("local")
      .appName("TfiRestaurants")
      .getOrCreate()
    
    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)

    val inputPath = "/home/denys/dar/data/tfi/train.csv"
    val data = readCsv(spark, inputPath)

    val numericCols = data.columns.filter(_.startsWith("P"))
    // entrainement avec uniquement des features numériques
    runTrainingAndEstimation(data, "revenue", numericCols)
    // entrainement avec features numériques et catégorielles
    runTrainingAndEstimation(data, "revenue", numericCols, Array("Type", "City Group"))
    
  }

}
