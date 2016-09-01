lazy val root = (project in file(".")).
settings(
  name := "scalaClassifier",
  version := "0.0.1",
  scalaVersion := "2.11.7",
  mainClass := Some("Main"),

   libraryDependencies += "org.apache.spark" %% "spark-core" % "1.6.0"
   //libraryDependencies += "org.apache.spark" %% "spark-mllib" % "1.5.0",
)
