name := "scala_test"

version := "0.1"

scalaVersion := "2.12.13"

libraryDependencies ++= Seq("org.apache.spark" % "spark-streaming_2.12" % "3.0.1",
  "org.apache.spark" % "spark-sql_2.12" % "3.0.1"
)
