package TempDS

import org.apache.spark.sql.DataFrame
import ot.dispatcher.sdk.{PluginCommand, PluginUtils}
import ot.dispatcher.sdk.core.SimpleQuery
import TempDSProc.filesToSingleDf


class ReadTempDS(sq: SimpleQuery, utils: PluginUtils) extends PluginCommand(sq, utils, Set("from", "to"))
                                                      with SparkSessionWrapper {

  override def transform(_df: DataFrame): DataFrame = {

    val tempDSPath: String = getKeyword("path").get

    // test
//    println(tempDsPath)
//    filesToSingleDf(tempDsPath).show()

    filesToSingleDf(tempDSPath)
  }
}
