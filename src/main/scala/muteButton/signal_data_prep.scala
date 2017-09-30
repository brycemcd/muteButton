package muteButton

import org.apache.spark.sql.SQLContext
import org.apache.spark.rdd.RDD
import org.apache.spark.ml.feature.StandardScaler
import org.apache.spark.mllib.linalg.Vector

object SignalDataPrep {
  lazy val allPoints = deriveAllPointsFromLabeledFreqs.cache()
  lazy val sc = SparkThings.sc
  lazy private val sqlContext = new SQLContext(sc)

  def deriveAllPointsFromLabeledFreqs : RDD[(Double, Vector)] = {
    val adfile = "/media/brycemcd/filestore/spark2bkp/football/supervised_samples/ad/freqs/ari_phi_chunked091_freqs-labeled.txt"
    //val adfile =  "hdfs://spark3.thedevranch.net/football/freqs/ad/all-labeled.txt"
    //val adfile =  "/media/brycemcd/filestore/spark2bkp/football/supervised_samples/ad/freqs/all-labeled.txt"

   val gamefile = "/media/brycemcd/filestore/spark2bkp/football/supervised_samples/game/freqs/ari_phi_chunked095_freqs-labeled.txt"
    //val gamefile = "hdfs://spark3.thedevranch.net/football/freqs/game/all-labeled.txt"
    //val gamefile =  "/media/brycemcd/filestore/spark2bkp/football/supervised_samples/game/freqs/all-labeled.txt"
    val trainAdLines = sc.textFile(adfile)
    val trainGameLines = sc.textFile(gamefile)

    val trainAdTouples = FrequencyIntensityRDD.convertFileContentsToMeanIntensities(trainAdLines)
    val trainGameTouples = FrequencyIntensityRDD.convertFileContentsToMeanIntensities(trainGameLines)

    val trainAdPoints = FrequencyIntensityRDD.convertFreqIntensToLabeledPoint(trainAdTouples, 1.0)
    val trainGamePoints = FrequencyIntensityRDD.convertFreqIntensToLabeledPoint(trainGameTouples, 0.0)

    println("training ad points " + trainAdPoints.count())
    println("training game points " + trainGamePoints.count())

    trainGamePoints.union(trainAdPoints)
  }

  def scaleFeatures(allPoints: RDD[(Double, Vector)],
                          scaledColumnName : String) = {

    println("scaling")
    val allPointsDF = sqlContext.createDataFrame(allPoints)
      .toDF("label", "rawfeatures")

    val scaler = new StandardScaler()
      .setInputCol("rawfeatures")
      .setOutputCol(scaledColumnName)
      .setWithStd(true)
      .setWithMean(false)

    val scalerModel = scaler.fit(allPointsDF)
    scalerModel.write.overwrite().save("models/scalerModel")
    scalerModel.transform(allPointsDF)
  }
}
