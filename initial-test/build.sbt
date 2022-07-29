name := "initial-test"
version := "0.1"
scalaVersion := "2.12.0"

// Spark 3.0
val sparkVersion = "3.0.1"
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core",
  "org.apache.spark" %% "spark-sql",
).map(_ % sparkVersion)

// https://mvnrepository.com/artifact/com.nvidia/rapids-4-spark
libraryDependencies += "com.nvidia" %% "rapids-4-spark" % "0.1.0"

// https://mvnrepository.com/artifact/ai.rapids/cudf
libraryDependencies += "ai.rapids" % "cudf" % "0.14"

// scala unit test
libraryDependencies += "org.scalactic" %% "scalactic" % "3.0.5"
libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.5" % "test"
//libraryDependencies += "com.nvidia" %% "rapids-4-spark-tests" % "0.2.0"
//libraryDependencies += "com.nvidia" %% "rapids-4-spark-integration-tests" % "0.2.0"
