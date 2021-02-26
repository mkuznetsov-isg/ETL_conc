package TempDS

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, lit}

import java.io.File
import scala.util.Try

class TempDSProc(val tempDsPath: String) {}

object TempDSProc extends SparkSessionWrapper {
  def apply(tempDsPath: String) = new TempDSProc(tempDsPath: String)

  def getFileList(path: String): Option[List[(String, String)]] = {
    val pathToDsStorage: File = new File(path)

    if (pathToDsStorage.exists && pathToDsStorage.isDirectory) {
      val list =
        pathToDsStorage
          .listFiles
          .filter(file => file.isFile && file.getName.endsWith(".jsonl"))
          .toList
          .map(file => (file.getName.split("/.jsonl")(0), file.toString))

      Option(list)
    } else None
  }

  def filesToListDf(files: Option[List[(String, String)]]): List[DataFrame] = {
    files match {
      case Some(list) =>
        list
          .map(file => (file._1, Try(spark.read.json(file._2))))
          .collect { case file if file._2.isSuccess => (file._1, file._2.get) }
          .map(timeColumnCheck)
      case None => List(spark.emptyDataFrame)
    }
  }

  private def timeColumnCheck(tup2: (String, DataFrame)): DataFrame = {
    val timestamp: Double = tup2._1.toDouble
    val df: DataFrame = tup2._2
    val timeColumn: String = "omds_ts"

    val timeColExist: Boolean = Try { df.select(timeColumn) }.isSuccess
    val resDf: DataFrame = if (timeColExist) {
      df.na.fill(timestamp, Seq(timeColumn))
    }  else {
      df.withColumn(timeColumn, lit(timestamp))
    }

    val sortCol = resDf.columns.sorted.map(col)
    resDf.select(sortCol: _*)
  }

  def mergeDf(df: List[DataFrame]): DataFrame =
    df.tail.foldLeft(df.head)(_ union _)

  def filesToSingleDf(path: String): DataFrame = {
    val fileList: Option[List[(String, String)]] = getFileList(path)
    val listDF: List[DataFrame] = filesToListDf(fileList)
    if (listDF.nonEmpty) mergeDf(listDF)
    else spark.emptyDataFrame
  }
}