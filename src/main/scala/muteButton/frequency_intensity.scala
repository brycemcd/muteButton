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
import scala.collection.mutable.ListBuffer
import scala.util.matching.Regex


import muteButton.NewTypes._

object FrequencyIntensityRDD {
  def mapFileToLabeledFreqIntensity(fileContents : RDD[String]) = {
    val freqIntensLines = """(\d{1,}\.\d{1,})  (\d{1,}\.\d{1,})  (.*freqs.*)""".r

    fileContents.flatMap {
        case freqIntensLines(freq, intense, seqNum) => Some( (seqNum, (freq.toDouble, intense.toDouble)) )
        case _ => None
    }
  }

  def convertFreqIntensToLabeledPoint(labeledTouples : RDD[LabeledFreqIntens], label : Double) = {
    println("CONVERTING")
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
    mapFileToLabeledFreqIntensity(fileContents)
  }
}

object FrequencyIntensityStreamWithList {
  val freqIntensLines = """(\d{1,}\.\d{1,})  (\d{1,}\.\d{1,})""".r

  def parseLine(line : String) = {
    (freqIntensLines findFirstIn line) match {
      case Some(str) =>
        val tup = str.split("  ")
        Some( (tup(0)toDouble, tup(1).toDouble) )
      case None =>
        None
    }
  }

  def isFirstLine(freqIntense : Tuple2[Double, Double]) : Boolean = {
    freqIntense match {
      case (0.0, _) => true
      case _ => false
    }
  }

  // TODO: make this a DStream
  def convertFileContentsToMeanIntensities(fileContents : DStream[String]) : DStream[List[LabeledFreqIntens]] = {
    val innerList = ListBuffer.empty[Tuple2[Double, Double]]

    def addToInnerList(tup : Tuple2[Double, Double]) = {
      tup match {
        case (freq, intense) => innerList += (freq -> intense)
      }
    }

    fileContents.flatMap { line =>
      val parsedLine = parseLine(line)
      parsedLine match {
        // a line without matches
        case None => None
        // first freq
        case Some(_) if isFirstLine(parsedLine.get) =>
          val innerStore = addFrequencyGroup(innerList)
          innerList.clear()
          addToInnerList(parsedLine.get)
          if(innerStore.size == 2048) Some(innerStore) else reportError(innerStore)
        case Some(_) =>
          addToInnerList(parsedLine.get)
          None
      }
    }
  }

  private def randomString : String = scala.util.Random.alphanumeric.take(10).mkString

  private def addFrequencyGroup(inner: ListBuffer[Tuple2[Double, Double]]) = {
    val random = "freqs" + randomString
    inner.toList.map { case (fre, int) => (random, (fre, int)) }
  }

  private def reportError(vari : Any) = {
    vari match {
      case str : String => if(!str.isEmpty) println("error: " + str)
      case li : List[(Double, Double)] => if(li.size != 0) println("error: " + li.size)
    }
    None
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
