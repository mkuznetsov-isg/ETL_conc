package TempDS

import Config.ConfigProc
import org.apache.spark.sql.DataFrame

object Test extends App {

  val conf: ConfigProc = ConfigProc("D:\\scala_work\\ETL_conc\\src\\main\\resources\\application.conf")
  val tempDsPath = conf.tempDsPath.getOrElse("")

  val tempDS: TempDSProc = TempDSProc(tempDsPath)

  import TempDSProc._

  val list: Option[List[(String, String)]] = getFileList(tempDS.tempDsPath)
  println(list.get)

  val dfList: List[DataFrame] = filesToListDf(list)
//  println(dfList.head.show)

  val bigDf: DataFrame = mergeDf(dfList)
  bigDf.show

}
