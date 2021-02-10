package Enrichment

import org.apache.spark.sql.DataFrame
import ot.dispatcher.sdk.{PluginCommand, PluginUtils}
import ot.dispatcher.sdk.core.SimpleQuery

import DataProc.listTDS
import TempDS.SparkSessionWrapper


class ListTDS(sq: SimpleQuery, utils: PluginUtils) extends PluginCommand(sq, utils, Set("from", "to"))
  with SparkSessionWrapper {

  override def transform(_df: DataFrame): DataFrame = {

    val argList: Map[String, String] = getKeywords

    val tDSPath: String =
      if (argList.nonEmpty && argList.contains("path")) getKeyword("path").get
      else utils.pluginConfig.getString("tdspath")

    // test
//    println(tDSPath)
//    listTDS(tDSPath).show(truncate = false)

    listTDS(tDSPath)
  }
}