package muteButton.Test

import org.scalatest._
import muteButton.StreamPrediction

import org.apache.spark.ml.classification.{LogisticRegressionModel}
import org.apache.spark.ml.feature.StandardScalerModel
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.linalg.Vector

import org.apache.spark.sql.DataFrame

class StreamPredictionSpec extends FunSpec {
  lazy val dataPoint = (0.0 , Vectors.zeros(2048) )

  describe("calcRatio") {
    lazy val number = 4

    it("returns 0 if diviser is 0 instead of being undefined") {
      val divisor = 0
      val ratio = StreamPrediction.calcRatio(number, divisor)
      assert(ratio == 0.0)
    }

    it("returns a number divided by divisor") {
      val divisor = 16
      val ratio = StreamPrediction.calcRatio(number, divisor)
      assert(ratio == 0.25)
    }

    it("accepts doubles") {
      val ratio = StreamPrediction.calcRatio(0.1, 0.01)
      assert( ratio == 10.0)
    }
  }

  describe("loading model") {
    it("loads a logistic regression model with a filepath string") {
      val model = StreamPrediction.defaultLogisticRegressionModel
      assert(model.isInstanceOf[LogisticRegressionModel])
    }
  }

  describe("scalerModel") {
    it("loads a scaler model") {
      val model = StreamPrediction.defaultScalerModel
      assert(model.isInstanceOf[StandardScalerModel])
    }
  }

  describe("makePredictions") {
    it("makes predictions based on the model and data provided") {
      val model = StreamPrediction.defaultLogisticRegressionModel
      val rawData  = StreamPrediction.dataToDataFrame( Seq(dataPoint) )
      val scaledData = StreamPrediction.defaultScalerModel.transform(rawData)
      val prediction = StreamPrediction.predictFromData(scaledData, model)

      assert(prediction.first().isInstanceOf[Tuple2[Double, Vector]])
    }
  }

}
