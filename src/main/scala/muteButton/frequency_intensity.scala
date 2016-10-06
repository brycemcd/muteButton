package muteButton

import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming._

import org.apache.log4j.Logger
import org.apache.log4j.Level

// A = DStream, B = ReceiverInputDStream for streaming things
// A = RDD, B =    for reading files
package object NewTypes {
  type FreqIntensities = (Double, (Double, Int))
  type LabeledFreqIntens = (String, (Double, Double))
}

import muteButton.NewTypes._
import scala.util.matching.Regex

object FrequencyIntensityRDD {
  def mapFileToLabeledFreqIntensity(fileContents : RDD[String]) = {
    val freqIntensLines = """(\d{1,}\.\d{1,})  (\d{1,}\.\d{1,})  (.*freqs.*)""".r

    fileContents.flatMap {
        case freqIntensLines(freq, intense, seqNum) => Some( (seqNum, (freq.toDouble, intense.toDouble)) )
        case _ => None
    }
  }

  def convertFreqIntensToLabeledPoint(labeledTouples : RDD[LabeledFreqIntens], label : Double) = {
    labeledTouples.groupByKey().map(_._2).map(_.toArray.sortBy(_._1)).map { x =>
      (label, Vectors.dense( x.map(_._2) ))
    }
  }

  def mapFileToFreqIntensityList(fileContents : RDD[String]) = {
    fileContents.map(_.split("  ")).filter(_.length == 2).map {
      case tup : Array[String] => (tup(0).toDouble, (tup(1).toDouble, 1))
    }.reduceByKey {
      (a : (Double, Int), v : (Double, Int)) => (a._1 + v._1, a._2 + v._2)
    }
  }

  def meanFrequencyIntensities(freqTuple : RDD[FreqIntensities]) : RDD[(Double, Double)] = {
    freqTuple.map {
      case (freq : Double, (sum : Double, count : Int)) => (freq, sum * 1/count)
    }.sortByKey(true)
  }

  //def convertFileContentsToMeanIntensities(fileContents : RDD[String]) : RDD[(Double, Double)] = {
  def convertFileContentsToMeanIntensities(fileContents : RDD[String]) = {
    //meanFrequencyIntensities(
      //mapFileToFreqIntensityList(fileContents)
    //)
    mapFileToLabeledFreqIntensity(fileContents)
  }
}

object FrequencyIntensityStreamWithList {
  def convertFileContentsToMeanIntensities(fileContents : DStream[String]) = {
    val firstLine = """(0.00\d{1,})  (\d{1,}\.\d{1,})""".r
    val freqIntensLines = """(\d{1,}\.\d{1,})  (\d{1,}\.\d{1,})""".r

    val innerList = scala.collection.mutable.ListBuffer.empty[Tuple2[Double, Double]]
    val outerList = scala.collection.mutable.ListBuffer.empty[List[LabeledFreqIntens]]

    def randomString : String = scala.util.Random.alphanumeric.take(10).mkString

    val res = fileContents.flatMap {
      case firstLine(freq, intense) =>
        val random = "freqs" + randomString

        val innerStore = innerList.toList.map { case (fre, int) => (random, (fre, int)) }
        if(innerStore.size == 2048) outerList += innerStore
        innerList.clear()
        innerList += (freq.toDouble -> intense.toDouble)
        //Some( outerList )
        if(innerStore.size == 2048) Some(innerStore) else None
      case freqIntensLines(freq, intense) =>
        //innerList += freq.toDouble
        innerList += (freq.toDouble -> intense.toDouble)
        None
      case _ => None
    }
    //map( outer => outer.filter(_.size == 2048) )
    res
  }
}
object FrequencyIntensityStream {

  // NOTE: this works because I have the guarantees:
  // 1. The data is being sent over sequentially in the right order
  // 2. The sequential data is processed on a single socket
  // 3. A single thread is looping through the fileContents in the same
  //    order that it is received on the network socket
  // if any of the above are not correct this does not work well
  def mapFileToFreqIntensityList(fileContents : DStream[String]) = {
    val freqIntensLines = """(\d{1,}\.\d{1,})  (\d{1,}\.\d{1,})""".r

    fileContents.flatMap {
        case freqIntensLines(freq, intense) => Some( (freq.toDouble, (intense.toDouble, 1) ))
        case _ => None
    }.reduceByKey {
      (a : (Double, Int), v : (Double, Int)) => (a._1 + v._1, a._2 + v._2)
    }
  }

  // works for streaming
  def meanFrequencyIntensities(freqTuple : DStream[FreqIntensities]) = {
    freqTuple.map {
      case (freq, (sum, count)) => (freq, sum * 1/count)
    }.transform(_.sortByKey(true))
  }

  def convertFileContentsToMeanIntensities(fileContents : DStream[String]) = {
    meanFrequencyIntensities(
      mapFileToFreqIntensityList(fileContents)
    )
  }
}
