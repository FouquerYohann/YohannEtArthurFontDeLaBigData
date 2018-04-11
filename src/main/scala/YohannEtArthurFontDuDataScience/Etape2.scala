package YohannEtArthurFontDuDataScience

import YohannEtArthurFontDuDataScience.HouseApp.loadData
import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.DecisionTreeClassifier
import org.apache.spark.ml.feature._
import org.apache.spark.sql
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, SQLContext, SparkSession}


object Etape2 extends App{


  def decisionTreeFiller(df: DataFrame, sqlContext: SQLContext, colu: String): sql.DataFrame = {

    var discreetCol = Array("buildingqualitytypeid", "heatingorsystemtypeid", "propertylandusetypeid",
      "regionidcity", "regionidzip", "regionidcounty")

    val dfNull = df.drop(Array("logerror","transactiondate"):_*).filter(col(colu).isNull).toDF()
    var dfNotNull = df.drop(Array("logerror","transactiondate"):_*).filter(!col(colu).isNull).toDF()

    discreetCol = discreetCol.filter(!_.equals(colu))

    dfNotNull = dfNotNull.na.fill(404, discreetCol)

//    dfNotNull.columns.foreach(println(_))

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
        .setMaxBins(10)
        .setMaxDepth(5)
        .setLabelCol("indexedLabel")
        .setFeaturesCol("features")

    val pipeline = new Pipeline().setStages(Array(labelIndexer, oneHotEncoder, VectorAssembler,dt))
    println("begining training")
    val model = pipeline.fit(dfNotNull).transform(dfNotNull)
    println("end training")


//    println("begining predictions training")
//    val predictions = model.transform(dfNull)
//    println("showing predictions")
//    predictions.show()

    df
  }


  override def main(args: Array[String]): Unit = {

    val path: String = "data/"

    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)


    val nbCore = Runtime.getRuntime().availableProcessors()
    println(s"nbCore = ${nbCore}")
    val sSession = SparkSession.builder().config("maxToStringFields",30).appName("YohannEtArthurFontDuDataScience")
                   .master(s"local[$nbCore]").getOrCreate()
    val sqlContext = sSession.sqlContext

    val fic = args(0)
    println("loading " + fic)
    var props = loadData(fic, sqlContext)
    println(fic + " loaded")

    props = props.sample(0.01)

    props = decisionTreeFiller(props,sqlContext,"buildingqualitytypeid")
  }

}
