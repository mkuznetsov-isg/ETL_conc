package Enrichment

import TempDS.SparkSessionWrapper
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.functions.max
import ot.dispatcher.sdk.core.SimpleQuery
import ot.dispatcher.sdk.{PluginCommand, PluginUtils}

import scala.collection.mutable.ArrayBuffer


class ReadTDS(sq: SimpleQuery, utils: PluginUtils) extends PluginCommand(sq, utils)
  with SparkSessionWrapper {

  val tdsPath: String = pluginConfig.getString("tdspath")
  val serviceCols: List[String] = List("_time")

  val address: String = """address\s*=\s*"(\S+)"""".r.findFirstMatchIn(args).get.group(1)
  val filter: String = """filter\s*=\s*"([^"]+)"""".r.findFirstMatchIn(args) match {
    case Some(g) => g.group(1)
    case None => ""
  }

  val metrics: String = getKeyword("metrics").getOrElse("")
  val tws: String = getKeyword("tws").getOrElse("0")
  val twf: String = getKeyword("tws").getOrElse("0")

  val bigIntZero = BigInt(0)

  def getLeafsByAddress(address: String): Array[String] = {
    val addressParts = address.split("\\*")
    val baseLeafs = DataProc.getArrayOfLeafs(tdsPath, addressParts.head)
    val leafs = addressParts.tail.foldLeft(baseLeafs)((bl, addressPart) => bl.filter(_.contains(addressPart)))
    leafs
  }

  def getMergedSchema(endDirs: List[String]): StructType = {
    val schema = DataProc.mergeSchemas(endDirs)
    schema
  }

  def convertTimeArgsToBigInt(t: String): BigInt = {
    BigInt(t)
  }

  def filterLeafsByTime(leafs: Array[String], tws: BigInt, twf: BigInt): Array[String] = {
    leafs.filter(leaf => {
      val leafTimeRanges = """_time_range=(\d+)-(\d+)""".r.findFirstMatchIn(leaf)

      leafTimeRanges match {
        case Some(ltr) =>
          val leafTWS = BigInt(ltr.group(1))
          val leafTWF = BigInt(ltr.group(2))

          (tws, twf) match {
            case (`bigIntZero`, `bigIntZero`) => true
            case (ts, `bigIntZero`) => if (ts <= leafTWF) true else false
            case (`bigIntZero`, tf) => if (tf >= leafTWS) true else false
            case (ts, tf) => if ((ts <= leafTWS && leafTWS <= tf) || (ts <=leafTWS && leafTWF <= tf) || (ts <= leafTWF && leafTWF <= tf)) true else false
            case _ => throw new Exception("Error in time window. Check args.")
          }
        case None => true
      }

    })
  }

  override def transform(_df: DataFrame): DataFrame = {
    var leafs = getLeafsByAddress(address)

    val twsMillis = convertTimeArgsToBigInt(tws)
    val twfMillis = convertTimeArgsToBigInt(twf)

    leafs = filterLeafsByTime(leafs, twsMillis, twfMillis)
    val objectTypeTag: String = pluginConfig.getString("objectTypeTag")

    val leafMap = collection.mutable.Map[String, ArrayBuffer[String]]()
    leafs.foreach(leaf => {
      val leafParts = f""".+?\\/$objectTypeTag=(.+?)\\/(.+)""".r.findFirstMatchIn(leaf).get
      val typeObjectPath = leafParts.group(1)
      leafMap.get(typeObjectPath) match {
        case Some(leafs) => leafs += leaf
        case None => leafMap(typeObjectPath) = ArrayBuffer(leaf)
      }
    })

    val dfs = leafMap.keys.map(key => {
      var schema = getMergedSchema(leafMap(key).toList)

      if (metrics != "") {
        val requiredColumns = metrics.split(", ?") ++ serviceCols
        schema = StructType(schema.filter(column => requiredColumns.contains(column.name)))
      }

      var df = spark
        .read
        .option("basePath", tdsPath)
        .format("parquet")
        .schema(schema)
        .load(leafMap(key):_*)

      val columns = df.schema.fieldNames

      val addressColumns = columns.filter(column => !schema.fieldNames.contains(column)) ++ serviceCols

      val columnNamePrefix = if (leafMap.keys.toList.length == 1) "" else f"$key."

      val aggFields = columns
        .filter(column => !addressColumns.contains(column))
        .map(column => max(column).alias(f"$columnNamePrefix$column"))

      if (aggFields.isEmpty) throw new Exception("No columns for aggregation. Check metrics and scheme.")
      df = df.groupBy(addressColumns.head, addressColumns.tail:_*).agg(aggFields.head, aggFields.tail:_*)

      df = (twsMillis, twfMillis) match {
        case (`bigIntZero`, `bigIntZero`) => df
        case (ts, `bigIntZero`) => df.filter(f"_time >= $ts")
        case (`bigIntZero`, tf) => df.filter(f"_time <= $tf")
        case (ts, tf) => df.filter(f"_time >= $ts and _time <= $tf")
        case _ => throw new Exception("Error in time window. Check args.")
      }

      df = if (filter == "") df else df.filter(filter)

      (df, addressColumns.toSeq)
    })

    var df = dfs.head._1
    dfs.tail.foreach(rdf => df = df.join(rdf._1, rdf._2, "full"))
    df

  }
}