package Enrichment

import TempDS.SparkSessionWrapper
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{Column, DataFrame}
import ot.dispatcher.sdk.core.SimpleQuery
import ot.dispatcher.sdk.{PluginCommand, PluginUtils}


class ReadTDS_old_3(sq: SimpleQuery, utils: PluginUtils) extends PluginCommand(sq, utils, Set("from", "to"))
  with SparkSessionWrapper {

  override def transform(_df: DataFrame): DataFrame = {

    import spark.implicits._

    def parseArgList(arr: Array[String]): Array[(String, String, String)] = {
      def parseOps(el: Array[String]): (String, String, String) = el match {
        case Array(arg, value, ops) => (arg, ops, value)
      }

      def parseValues(el: (String, String, String)): (String, String, String) = el match {
        case (arg, ops, value) if (arg == "ds" || arg == "metrics" || arg == "actual_time") && ops == "=" =>
          (arg, "notCondition", value)
        case (arg, ops, value) if (arg == "tws") && ops == "=" =>
          ("_time", ">=", value)
        case (arg, ops, value) if (arg == "twf") && ops == "=" =>
          ("_time", "<=", value)
        case (arg, ops, value) if value == "*" && ops == "=" =>
          (arg, "notCondition", "*")
        case (arg, ops, value) if value.endsWith("*") && ops == "=" =>
          (arg, " like ", s"%${value.replace("*", "")}%")
        case (arg, ops, value) if value.endsWith("*") && ops == "!=" =>
          (arg, " not like ", s"%${value.replace("*", "")}%")
        case el => el
      }

      arr
        .map {
          case el if el.split(">=").length > 1 =>
            parseOps(el.split(">=") :+ ">=")
          case el if el.split("<=").length > 1 =>
            parseOps(el.split("<=") :+ "<=")
          case el if el.split("!=").length > 1 =>
            parseOps(el.split("!=") :+ "!=")
          case el if el.split("=").length > 1 =>
            parseOps(el.split("=") :+ "=")
          case el if el.split(">").length > 1 =>
            parseOps(el.split(">") :+ ">")
          case el if el.split("<").length > 1 =>
            parseOps(el.split("<") :+ "<")
        }
        .map(parseValues)
    }

    val args1: String = args
    println(args1)

    val stringForParsing: Array[String] = args
      .replaceAll("^\\s+", "")
      .replaceAll(",\\s+", ",")
      .replaceAll("\\s+", " ")
      .replaceAll("\"", "")
      .split("\\s")

    println(stringForParsing.mkString("Array(", ", ", ")"))

    val parsed: Array[(String, String, String)] = parseArgList(stringForParsing)
    println(parsed.mkString(" "))

    val sparkCondition: String =
      parsed
        .filter(_._2 != "notCondition")
        .foldLeft("")((acc, el) => el match {
          case (arg, ops, value) if value.split(",").length > 1 =>
            acc.concat(value.split(",").map(v => s"$arg$ops'$v'").mkString("("," or ",")").concat(" and "))
          case el => acc.concat(s"${el._1}${el._2}'${el._3}' and ")
        })
        .dropRight(5)

    println(sparkCondition)

    val fields: Array[String] =
      parsed
        .flatMap(el =>
          if (el._1 != "metrics") Array(el._1)
          else el._3.split(","))

    println(fields.mkString("Array(", ", ", ")"))

    val ds: String =
      if (parsed.exists(_._1 == "ds")) parsed.filter(_._1 == "ds")(0)._3
      else "tds,fds"

    println(ds)

    val actualTime: Boolean =
      if (parsed.exists(_._1 == "actual_time")) parsed.filter(_._1 == "actual_time")(0)._3.toBoolean
      else false

    println(actualTime)

    val excludeFromMetrics: Array[String] = Array("ds", "actual_time", "tws", "twf")

    val metrics: List[Column] =
      if (parsed.filter(_._1 == "metrics")(0)._3 == "*") Nil
      else {
        val res: List[Column] = fields.toList.filterNot(excludeFromMetrics.contains).map(col).distinct
        if (res.contains($"_time")) res else res :+ col("_time")
      }

    println(metrics)

    val start: Long = System.nanoTime()

    val res: DataFrame = if (metrics == Nil)
      spark
        .read
        .parquet("D:\\res\\megion\\fond_lg201911")
        .filter(sparkCondition)
    else
      spark
        .read
        .parquet("D:\\res\\megion\\fond_lg201911")
        .filter(sparkCondition)
        .select(metrics: _*)

    val stop: Long = System.nanoTime()
    println(stop - start)

    res.show(truncate = false)
    res.printSchema
    res.explain(extended = true)

    spark.emptyDataFrame

  }
}