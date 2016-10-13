package muteButton

import sys.process._
object PredictionAction {
  val negativeCase = () => toggleSystem("unmute")
  val positiveCase = () => toggleSystem("mute")

  private def toggleSystem(soundControl : String) : Int = {
    "sudo amixer set Speaker " + soundControl !
  }

  def ratioBasedMuteAction(threshold : Double, ad_ratio : Double) = {
    if(ad_ratio < threshold) negativeCase() else positiveCase()
  }
}
