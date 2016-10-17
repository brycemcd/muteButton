package muteButton
import org.apache.spark.mllib.linalg.Vector

package object NewTypes {
  type FreqIntensities = (Double, (Double, Int))
  type LabeledFreqIntens = (String, (Double, Double))
  type PredictTuple = (Double, Vector)
}

