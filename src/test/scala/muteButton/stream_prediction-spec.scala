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
      val model = StreamPrediction.logisticRegressionModel
      assert(model.isInstanceOf[LogisticRegressionModel])
    }
  }

  describe("scalerModel") {
    it("loads a scaler model") {
      val model = StreamPrediction.scalerModel
      assert(model.isInstanceOf[StandardScalerModel])
    }
  }
}
