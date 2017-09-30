package muteButton

import org.apache.spark.sql.SQLContext
import org.apache.spark.rdd.RDD
import org.apache.spark.ml.feature.StandardScaler
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.sql.Row
import org.apache.spark.sql.Dataset

object SignalDataPrep {
  lazy val allPoints = deriveAllPointsFromLabeledFreqs.cache()
  lazy val sc = SparkThings.sc
  lazy private val sqlContext = SparkThings.sqlContext //new SQLContext(sc)

  def deriveAllPointsFromLabeledFreqs : Dataset[Row] = {
    val adfile = "/media/brycemcd/filestore/spark2bkp/football/supervised_samples/ad/freqs/ari_phi_chunked091_freqs-labeled.txt"
    //val adfile =  "hdfs://spark3.thedevranch.net/football/freqs/ad/all-labeled.txt"
    //val adfile =  "/media/brycemcd/filestore/spark2bkp/football/supervised_samples/ad/freqs/all-labeled.txt"

   val gamefile = "/media/brycemcd/filestore/spark2bkp/football/supervised_samples/game/freqs/ari_phi_chunked095_freqs-labeled.txt"
    //val gamefile = "hdfs://spark3.thedevranch.net/football/freqs/game/all-labeled.txt"
    //val gamefile =  "/media/brycemcd/filestore/spark2bkp/football/supervised_samples/game/freqs/all-labeled.txt"
    val trainAdLines = sc.textFile(adfile)
    val trainGameLines = sc.textFile(gamefile)

    val trainAdTuples = FrequencyIntensityRDD.convertFileContentsToMeanIntensities(trainAdLines)
    val trainGameTuples = FrequencyIntensityRDD.convertFileContentsToMeanIntensities(trainGameLines)

    val trainAdPoints = FrequencyIntensityRDD.convertFreqIntensToLabeledPoint(trainAdTuples, 1.0)
    val trainGamePoints = FrequencyIntensityRDD.convertFreqIntensToLabeledPoint(trainGameTuples, 0.0)

    println("training ad points " + trainAdPoints.count())
    println("training game points " + trainGamePoints.count())

    import sqlContext.implicits._
    trainGamePoints.union(trainAdPoints).toDF("label", "rawFeatures")
  }

  def scaleFeatures(scaledColumnName : String) = {

    println("scaling")
    val scaler = new StandardScaler()
      .setInputCol("rawFeatures")
      .setOutputCol(scaledColumnName)
      .setWithStd(true)
      .setWithMean(false)

    val scalerModel = scaler.fit(allPoints)
    scalerModel.write.overwrite().save("models/scalerModel")
    scalerModel.transform(allPoints)
  }
}
