package muteButton
import org.apache.spark.ml.linalg.{Vector, Vectors}

package object NewTypes {
  type FreqIntensities = (Double, (Double, Int))
  type LabeledFreqIntens = (String, (Double, Double))
  type PredictTuple = (Double, Vector)
}

