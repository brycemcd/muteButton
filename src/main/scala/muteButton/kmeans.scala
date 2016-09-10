package muteButton

import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.SparkContext._
import org.apache.spark.mllib.clustering.{KMeans, KMeansModel}

object KMeansModelDERP {
  def train() = {
    //val lines = sc.textFile("/media/brycemcd/filestore/spark2bkp/football/freqs/exp/*-labeled.txt")
    val numClusters = 2
    val numIterations = 20

    // TODO: make this work
    //val model = KMeans.train(groupedFrequenciesRDD, numClusters, numIterations)

    //val modelSaveString = "models/kmeans.model-" + System.currentTimeMillis()
    //model.save(sc, modelSaveString)

    //println( model.toPMML() )
  }
}
