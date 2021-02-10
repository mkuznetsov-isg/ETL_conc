package Enrichment

import TempDS.SparkSessionWrapper
import org.apache.spark.sql.DataFrame
import ot.dispatcher.sdk.core.SimpleQuery
import ot.dispatcher.sdk.{PluginCommand, PluginUtils}


class ReadTDS_old_1(sq: SimpleQuery, utils: PluginUtils) extends PluginCommand(sq, utils, Set("from", "to"))
  with SparkSessionWrapper {

  override def transform(_df: DataFrame): DataFrame = {

    def parseArgList(arr: Array[String]): Array[(String, String, String)] = {
      def parseOps(el: Array[String]): (String, String, String) = el match {
        case Array(arg, value, ops) => (arg, ops, value)
      }

      def parseValues(el: (String, String, String)): (String, String, String) = el match {
        case (arg, ops, value) if (arg == "ds" || arg == "metrics" || arg == "actual_time") && ops == "===" =>
          val res = if (value.startsWith("\"") && value.endsWith("\"")) value.replaceAll("\"", "")
          else value
          (arg, "notCondition", res)
        case (arg, ops, value) if value == "*" && ops == "===" =>
          (arg, "notCondition", "*")
        case (arg, ops, value) if value.startsWith("\"") && value.endsWith("\"") =>
          (arg, ops, value.replaceAll("\"", ""))
        case (arg, ops, value) if value == "*" && ops == "===" =>
          (arg, "notCondition", "*")
        case (arg, ops, value) if value.endsWith("*") && ops == "===" =>
          (arg, ".contains", s"(${"\""}${value.replace("*", "")}${"\""})")
        case (arg, ops, value) if value.endsWith("*") && ops == "=!=" =>
          (arg, "notContains", s"(${"\""}${value.replace("*", "")}${"\""})")
        case el => el
      }

      arr
        .map {
          case el if el.split(">=").length > 1 =>
            parseOps(el.split(">=") :+ ">=")
          case el if el.split("<=").length > 1 =>
            parseOps(el.split("<=") :+ "<=")
          case el if el.split("!=").length > 1 =>
            parseOps(el.split("!=") :+ "=!=")
          case el if el.split("=").length > 1 =>
            parseOps(el.split("=") :+ "===")
          case el if el.split(">").length > 1 =>
            parseOps(el.split(">") :+ ">")
          case el if el.split("<").length > 1 =>
            parseOps(el.split("<") :+ "<")
        }.map(parseValues)
    }

    val args1 = args
    println(args1)

    val argWhitespaces = args
      .replaceAll("^\\s+", "")
      .replaceAll(",\\s+", ",")
      .replaceAll("\\s+", " ")
      .split("\\s")

    println(argWhitespaces.mkString("Array(", ", ", ")"))

    val parsed: Array[(String, String, String)] = parseArgList(argWhitespaces)
    println(parsed.mkString(" "))

    val sparkCondition =
      parsed
        .filter(_._2 != "notCondition")
        .foldLeft("")((acc, el) => el match {
          case (arg, ops, value) if value.split(",").length > 1 =>
            acc.concat(value.split(",").map(v => s"${"$"}${"\""}$arg${"\""}$ops$v").mkString(" || ").concat(" && "))
          case (arg, ops, value) if ops == "notContains" =>
            acc.concat(s"!${"$"}${"\""}$arg${"\""}.contains($value) && ")
          case el => acc.concat(s"${"$"}${"\""}${el._1}${"\""}${el._2}${el._3} && ")
        })
    println(sparkCondition)

    val finalSparkCondition = if (sparkCondition.endsWith(" && ")) sparkCondition.dropRight(4) else sparkCondition
    println(finalSparkCondition)

    val fields = parsed.flatMap(el => if (el._1 != "metrics") Array(el._1) else el._3.split(","))
    println(fields.mkString("Array(", ", ", ")"))

    val ds = if (parsed.exists(_._1 == "ds")) parsed.filter(_._1 == "ds")(0)._3 else "tds,fds"
    println(ds)
    val actualTime = if (parsed.exists(_._1 == "actual_time")) parsed.filter(_._1 == "actual_time")(0)._3.toBoolean else false
    println(actualTime)

    val excludeFromMetrics = Array("ds", "actual_time")
    val metrics =
      if (parsed.filter(_._1 == "metrics")(0)._3 == "*") "all"
      else parsed.filter(_._1 == "metrics")(0)._3.filterNot(excludeFromMetrics.contains)
    println(metrics)

    val res = if (metrics == "all")
      spark
        .read
        .parquet("D:\\res\\megion\\fond_lg201911")
        .filter(finalSparkCondition)
    else
      spark
        .read
        .parquet("D:\\res\\megion\\fond_lg201911")
        .filter(sparkCondition)
        .select(finalSparkCondition)

    res.show

    res







  }
}