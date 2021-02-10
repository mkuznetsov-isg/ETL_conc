name := "ETL_conc"

version := "0.1"

scalaVersion := "2.11.12"

ThisBuild / useCoursier := false

// https://mvnrepository.com/artifact/com.typesafe/config
libraryDependencies += "com.typesafe" % "config" % "1.4.1"

// https://mvnrepository.com/artifact/org.apache.logging.log4j/log4j-core
libraryDependencies += "org.apache.logging.log4j" % "log4j-core" % "2.14.0"

// https://mvnrepository.com/artifact/org.apache.logging.log4j/log4j-api
libraryDependencies += "org.apache.logging.log4j" % "log4j-api" % "2.14.0"

// https://mvnrepository.com/artifact/org.apache.logging.log4j/log4j-api-scala
libraryDependencies += "org.apache.logging.log4j" %% "log4j-api-scala" % "12.0"

// https://mvnrepository.com/artifact/org.quartz-scheduler/quartz
libraryDependencies += "org.quartz-scheduler" % "quartz" % "2.3.2"

val sparkVersion = "2.4.3"

// https://mvnrepository.com/artifact/org.apache.spark/spark-core
// https://mvnrepository.com/artifact/org.apache.spark/spark-sql
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core",
  "org.apache.spark" %% "spark-sql"
).map(_ % sparkVersion)

libraryDependencies += "ot.dispatcher" % "dispatcher_2.11" % "1.5.2" % Compile

// https://mvnrepository.com/artifact/com.cronutils/cron-utils
libraryDependencies += "com.cronutils" % "cron-utils" % "9.1.3"

scalacOptions in ThisBuild += "-target:jvm-1.8"

// https://mvnrepository.com/artifact/org.json4s/json4s-jackson
//libraryDependencies += "org.json4s" %% "json4s-jackson" % "3.6.10"