package Enrichment

import TempDS.SparkSessionWrapper
import org.apache.spark.sql.{ColumnName, DataFrame}

object Test4 extends App with SparkSessionWrapper{

  import spark.implicits._

  val fields: Seq[ColumnName] = Seq($"_year", $"_month")

  val df:DataFrame = spark
    .read
//    .parquet("D:\\res\\tds")
//    .parquet("D:\\res\\tds\\_year=2020-2020\\***\\***\\_time_range=1607634000-1608498000")
    .parquet("D:\\res\\tds\\_year=2020-2020\\***\\***\\_time_range=1607634000-1608498000")
//    .parquet("D:\\res\\tds\\_year=2020-2020\\**\\_time_range=1607634000-1608498000")
//    .filter($"_year" === "2020-2020")
//    .select(fields: _*)

//  "D:\\res\\tds\\_year=2020-2020\\_month=12-12\\_day=11-21\\_time_range=1607634000-1608498000\\x=10\\y=20"

//  df.explain()

  df.show

//  df
//    .where($"_year".contains("2020") && $"x" === "10" && $"omds_status" === "new")
//    .explain

}
