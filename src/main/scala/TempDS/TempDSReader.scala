package TempDS

import org.apache.spark.sql.{DataFrame, SparkSession}
import ot.dispatcher.sdk.{PluginCommand, PluginUtils}
import ot.dispatcher.sdk.core.SimpleQuery
import TempDSProc.filesToSingleDf


class TempDSReader(sq: SimpleQuery, utils: PluginUtils) extends PluginCommand(sq, utils) {

  override def transform(_df: DataFrame): DataFrame = {

    val tempDSPath: String = getKeyword("path").getOrElse("")

    // test
//    println(tempDsPath)
//    filesToSingleDf(tempDsPath).show()

    val spark = SparkSession.builder().getOrCreate()

    tempDSPath match {
      case "" => spark.emptyDataFrame
      case path => {
        filesToSingleDf(tempDSPath)
      }
    }

  }
}
