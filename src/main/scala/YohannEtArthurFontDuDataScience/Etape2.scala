package YohannEtArthurFontDuDataScience

import YohannEtArthurFontDuDataScience.HouseApp.loadData
import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.DecisionTreeClassificationModel
import org.apache.spark.ml.classification.DecisionTreeClassifier
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature._
import org.apache.spark.mllib.tree.DecisionTree
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.sql
import org.apache.spark.sql.functions.{col, mean}
import org.apache.spark.sql.{DataFrame, SQLContext, SparkSession}


object Etape2 extends App{


  def decisionTreeFiller(df: DataFrame, sqlContext: SQLContext, colu: String): sql.DataFrame = {

    var discreetCol = Array("buildingqualitytypeid", "heatingorsystemtypeid", "propertylandusetypeid",
      "regionidcity", "regionidzip", "regionidcounty")

    val dfNull = df.drop(Array("logerror","transactiondate"):_*).filter(col(colu).isNull).toDF()
    var dfNotNull = df.drop(Array("logerror","transactiondate"):_*).filter(!col(colu).isNull).toDF()

    discreetCol = discreetCol.filter(!_.equals(colu))

    dfNotNull = dfNotNull.na.fill(404, discreetCol)

    dfNotNull.columns.foreach(println(_))

    val whatShouldBeLeft = Array("bathroomcnt", "bedroomcnt", "calculatedbathnbr", "calculatedfinishedsquarefeet",
      "finishedsquarefeet12", "fireplacecnt", "fullbathcnt", "garagetotalsqft", "latitude", "longitude",
      "lotsizesquarefeet", "poolsizesum", "roomcnt", "unitcnt", "yearbuilt", "structuretaxvaluedollarcnt",
      "taxvaluedollarcnt", "assessmentyear", "landtaxvaluedollarcnt", "taxamount", "taxdelinquencyyear")


    val labelIndexer = new StringIndexer()
        .setInputCol(colu)
        .setOutputCol("indexedLabel")
        .fit(dfNotNull)

    val oneHotEncoder = new OneHotEncoderEstimator().setInputCols(discreetCol)
                                                    .setOutputCols(discreetCol.map(_ + "vect")).fit(dfNotNull)

    val VectorAssembler = new VectorAssembler().setInputCols(discreetCol.map(_ + "vect") ++ whatShouldBeLeft)
                                               .setOutputCol("features")

    val labelConverter = new IndexToString()
        .setInputCol("prediction")
        .setOutputCol("predictedLabel")
        .setLabels(labelIndexer.labels)


    // Train a DecisionTree model.
    val dt = new DecisionTreeClassifier()
        .setLabelCol("indexedLabel")
        .setFeaturesCol("features")

    val pipeline = new Pipeline().setStages(Array(labelIndexer, oneHotEncoder, VectorAssembler, dt, labelConverter))

    val model = pipeline.fit(dfNotNull)

    val predictions = model.transform(dfNull)

    predictions.show()

    df
  }


  override def main(args: Array[String]): Unit = {

    val path: String = "data/"

    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)


    val nbCore = Runtime.getRuntime().availableProcessors()
    println(s"nbCore = ${nbCore}")
    val sSession = SparkSession.builder().appName("YohannEtArthurFontDuDataScience")
                   .master(s"local[$nbCore]").getOrCreate()
    val sqlContext = sSession.sqlContext

    val fic = "data/NumericFilledWithMean2017.csv"
    println("loading " + fic)
    var props = loadData(path + fic, sqlContext)
    println(fic + " loaded")


    props = decisionTreeFiller(props,sqlContext,"buildingqualitytypeid")
  }

}
