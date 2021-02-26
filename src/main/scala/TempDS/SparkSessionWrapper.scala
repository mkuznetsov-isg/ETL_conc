package TempDS

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

trait SparkSessionWrapper extends Serializable {

  lazy val spark: SparkSession =
    SparkSession
      .builder
      .appName("embedded log4j test")
      .master("local[*]")
      .getOrCreate()

  Logger.getLogger("org").setLevel(Level.OFF)

}