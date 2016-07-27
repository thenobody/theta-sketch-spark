name := "theta-sketch-spark"

version := "1.0"

scalaVersion := "2.11.8"

libraryDependencies ++= {
  Seq(
    "com.yahoo.datasketches" % "sketches-core" % "0.4.1"
  )
}
    