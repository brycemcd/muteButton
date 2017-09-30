lazy val commonSettings = Seq(
  // organization := "org.myproject"
  name := "muteButton",
  version := "0.0.1"
)
lazy val root = (project in file(".")).
  settings(commonSettings: _*).
  settings(
    scalaVersion := "2.11.11",
    //mainClass := Some("Main"),

    pollInterval := 1000,
    logLevel in (Compile, run):= Level.Warn,
    logLevel in (test):= Level.Debug,

    resolvers += "Artima Maven Repository" at "http://repo.artima.com/releases",
    resolvers += "Sonatype OSS Snapshots" at "https://oss.sonatype.org/content/repositories/snapshots",

    scalaSource in Compile := baseDirectory.value / "src" / "main",
    scalaSource in Test := baseDirectory.value / "src" / "test",

    libraryDependencies += "com.storm-enroute" %% "scalameter-core" % "0.7",
    libraryDependencies += "org.apache.spark" %% "spark-core" % "2.2.0",
    libraryDependencies += "org.apache.spark" %% "spark-streaming" % "2.2.0",
    libraryDependencies += "org.apache.spark" %% "spark-mllib" % "2.2.0",
    libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.2.0",

    libraryDependencies += "org.scalactic" %% "scalactic" % "3.0.0",
    libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.0" % "test"
  )
