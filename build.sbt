name := "RatingFinal"

version := "0.1"

scalaVersion := "2.11.12"

libraryDependencies += "org.apache.spark" %% "spark-core" % "2.2.1"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.2.1"
libraryDependencies += "com.databricks" %% "spark-redshift" % "3.0.0-preview1"
libraryDependencies += "com.databricks" %% "spark-avro" % "4.0.0"
libraryDependencies += "com.typesafe" % "config" % "1.3.3"
libraryDependencies += "junit" % "junit" % "4.12" % Test
/*libraryDependencies += "com.amazonaws" % "aws-java-sdk" % "1.11.286"
libraryDependencies += "org.apache.hadoop" % "hadoop-aws" % "2.8.3"
libraryDependencies += "org.apache.hadoop" % "hadoop-hdfs" % "2.8.3"*/
resolvers += "jitpack" at "https://jitpack.io"