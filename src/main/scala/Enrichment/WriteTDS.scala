package Enrichment

import Enrichment.DataProc.writeTDS
import TempDS.SparkSessionWrapper
import org.apache.spark.sql.DataFrame
import ot.dispatcher.sdk.core.SimpleQuery
import ot.dispatcher.sdk.{PluginCommand, PluginUtils}


class WriteTDS(sq: SimpleQuery, utils: PluginUtils) extends PluginCommand(sq, utils, Set("from", "to"))
  with SparkSessionWrapper {

  override def transform(_df: DataFrame): DataFrame = {

    val argList: Map[String, String] = getKeywords

    val tDSPath: String =
      if (argList.nonEmpty && argList.contains("path")) getKeyword("path").get
      else utils.pluginConfig.getString("tdspath")

    val partFields: String =
      if (argList.nonEmpty && argList.contains("pfields")) getKeyword("pfields").get
      else utils.pluginConfig.getString("tdspfields")

    val partFieldsList: List[String] =
      if (partFields.contains(",")) partFields.split(",").toList
      else List(partFields)

    // test
//    val all = getKeywords()
//    println(all)
//    println(tDSPath)
//    println(partFields)
//    println(partFieldsList)
//
//    import spark.implicits._
//
//    val _df1 = Seq(
//      ("tds", 1),
//      ("_year=2020-2020", 2),
//      ("_month=12-12", 3),
//      ("_day=11-21", 4),
//      ("_time_range=1607634000-1608498000", 5),
//      ("+--x=10", 6),
//      ("+--y=20", 7)
////      ("+--tds/", 1),
////      ("|  +--_year=2020-2020/", 2),
////      ("|  |  +--_month=12-12/", 3),
////      ("|  |  |  +--_day=11-21/", 4),
////      ("|  |  |  |  +--_time_range=1607634000-1608498000/", 5),
////      ("|  |  |  |  |  +--x=10/", 6),
////      ("|  |  |  |  |  |  +--y=20/", 7)
//    ).toDF("path", "value")
//    val tDSPath1 = "D:\\res\\part_test"
//    val partFieldsList1 = List("path")
//
//    writeTDS(_df1, tDSPath1, partFieldsList1)

    writeTDS(_df, tDSPath, partFieldsList)

    spark.emptyDataFrame
  }
}