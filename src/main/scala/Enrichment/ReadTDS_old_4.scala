package Enrichment

import Enrichment.DataProc.getFilesList
import TempDS.SparkSessionWrapper
import org.apache.spark.sql.DataFrame
import ot.dispatcher.sdk.core.{Keyword, SimpleQuery}
import ot.dispatcher.sdk.{PluginCommand, PluginUtils}

import scala.collection.immutable


class ReadTDS_old_4(sq: SimpleQuery, utils: PluginUtils) extends PluginCommand(sq, utils, Set("from", "to"))
  with SparkSessionWrapper {

  override def transform(_df: DataFrame): DataFrame = {

    import spark.implicits._

    val args1: String = args
    println(s"$args1\n")

    val keys: List[String] = List("ds", "tws", "twf", "metrics")

    val newArgs: String = args
      .replaceAll("^\\s+", "")
      .replaceAll(",\\s+", ",")
      .replaceAll("\\s+", " ")
      .replaceAll("\"", "")
    println(s"$newArgs\n")

    val parsedArgs: Map[String, String] = keywordsParser(newArgs)
      .map{case Keyword(k,v) => k -> v}.toMap
      .filterKeys(key => keys.contains(key))
    println(s"$parsedArgs\n")

    val exclude: immutable.Iterable[String] = parsedArgs
      .map(el => s"${el._1}=${el._2}")
    println(s"$exclude\n")

    val have2Parse: String = exclude
      .foldLeft(newArgs)((acc, str) => acc.replace(str, ""))
      .trim
    println(s"$have2Parse\n")

    val parse1: Array[String] = have2Parse
      .split(" ")
    println(s"${parse1.mkString(", ")}\n")

    val address: String = parse1.filter(_.startsWith("address"))(0)
    println(s"$address\n")

    val fields: Array[String] = address
      .replace("address=/", "")
      .replaceAll("/", " ")
      .split(" ")
      .filter(el => !(el == "**"))
    println(s"${fields.mkString(" ")}\n")

    val partitions = fields
      .filter(el => !el.contains("*"))
      .map { el =>
        if (el.contains(",")) {
          val split1: Array[String] = el.split("=")
          val arg: String = split1(0)
          val values: Array[String] = split1(1).split(",").map(el => s"$arg=$el")
          values.toList
        }
        else List(el)
      }.toList
    println(s"${partitions}\n")

    val sparkCondition = parse1.filter(! _.contains("address")).mkString(" ")
    println(s"$sparkCondition\n")

    val files: List[String] = getFilesList("D:\\res\\megion\\fond_lg201911")
    println(s"${files.size}\n")

//    val filteredFiles =
//      partitions.foldLeft(files)((acc, predicate) =>
//        acc.filter(el => predicate.exists(pr => el.matches(s".*$pr")))
//      )

//    println(s"${filteredFiles.size}\n")


//    println(s"$files\n")





    spark.emptyDataFrame

  }
}