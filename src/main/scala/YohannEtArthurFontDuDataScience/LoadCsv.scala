package YohannEtArthurFontDuDataScience

import YohannEtArthurFontDuDataScience.HouseApp.loadData
import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.mllib.evaluation.RegressionMetrics
import org.apache.spark.sql.functions.mean
import org.apache.spark.sql.{DataFrame, SQLContext, SparkSession}





object LoadCsv extends App {
  def linearRegression(df: DataFrame, sqlContext: SQLContext): PipelineModel = {

    val numeriCols = Array(
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
      "yearbuilt"
    ).filter(df.columns.contains(_))

    val assembler = new VectorAssembler()
      .setInputCols(numeriCols)
      .setOutputCol("features")

    val lr = new LinearRegression().setLabelCol("logerror")
    val pipeline = new Pipeline()
      .setStages(Array(assembler, lr))


    pipeline.fit(df.na.fill(df.columns.filter(numeriCols.contains(_)).zip(
      df.select(df.columns.filter(numeriCols.contains(_)).map(mean(_)): _*).first.toSeq
    ).toMap))
  }

  override def main(args: Array[String]): Unit = {
    val path: String = "data/"

    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)

    val nbCore = Runtime.getRuntime().availableProcessors()
    println(s"nbCore = ${nbCore}")
    val sSession = SparkSession.builder().appName("YohannEtArthurFontDuDataScience")
      .master(s"local[${nbCore}]").getOrCreate()
    val sqlContext = sSession.sqlContext

    val fic = "NumericFilledWithMeanAndStatisticDistribution2017.csv"
    println("loading " + fic)
    var props = loadData(path + fic, sqlContext)
    println(fic + " loaded")

    val modelLR = linearRegression(props, sqlContext)

    props = modelLR.transform(props.na.drop())

    val predictionAndObservations = props
      .select("prediction", Array("logerror"): _*)
      .rdd
      .map(row => (row.getDouble(0), row.getDouble(1)))
    val metrics = new RegressionMetrics(predictionAndObservations)
    val rmse = metrics.rootMeanSquaredError
    println("RMSE: " + rmse)

  }
}
