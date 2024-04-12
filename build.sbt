ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.13"

lazy val root = (project in file("."))
  .settings(
    name := "Airbnb Spark Analytics"
  )

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "3.4.2",
  "org.apache.spark" %% "spark-sql" % "3.4.2",
  "org.apache.spark" %% "spark-mllib" % "3.4.2",
  "org.apache.spark" %% "spark-streaming" % "3.4.2",
  "net.snowflake" %% "spark-snowflake" % "2.15.0-spark_3.4"
)