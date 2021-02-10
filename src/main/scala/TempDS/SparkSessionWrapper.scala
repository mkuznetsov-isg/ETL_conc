package TempDS

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

trait SparkSessionWrapper extends Serializable {

  lazy val spark: SparkSession =
    SparkSession
      .builder()
      .master("local[*]")
      .appName("app")
      .getOrCreate()

  Logger.getLogger("org").setLevel(Level.OFF)

}