package Enrichment

import Enrichment.DataProc.getFilesList
import TempDS.SparkSessionWrapper
import TempDS.TempDSProc._
import com.cronutils.model.definition.{CronDefinition, CronDefinitionBuilder}
import com.cronutils.parser.CronParser
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, udf}
import ot.dispatcher.OTLQuery
import ot.scalaotl.Converter
import com.cronutils.model.CronType.QUARTZ
import com.cronutils.model.time.ExecutionTime
import org.apache.spark.sql.types.StructType

import java.nio.file.{FileSystems, Files, Path, Paths}
import java.io.File
import scala.collection.JavaConverters._
import java.time.{Instant, ZoneId, ZonedDateTime}
import java.util.Optional
import scala.util.Random
import org.json4s.jackson.JsonMethods._
import org.json4s.jackson.Serialization.write
import org.json4s.{DefaultFormats, JValue}

import scala.io.{BufferedSource, Source}
import scala.reflect.io.{File => refFile}

class DataProc(
               private val enrichQs: List[String],
               private val validateQs: List[String],
               private val tempDs: String,
               private val partFields: List[String]
              ) extends SparkSessionWrapper {

  private val timeCol: List[String] = List("_year", "_month", "_day", "_time_range")

  def process(): (DataFrame, DataFrame) = {
    val resDf: DataFrame =  filesToSingleDf(tempDs)
    resDf.show

    // может ли быть только энрич или только валидация ???
    if (!resDf.isEmpty && enrichQs.nonEmpty && validateQs.nonEmpty) {
      val enrichedDf: DataFrame = transform(enrichQs, resDf)
      enrichedDf.cache
//      enrichedDf.show
      val validDf: DataFrame = transform(validateQs, enrichedDf)
//      validDf.show
      val invalidDf: DataFrame = enrichedDf.except(validDf)
//      invalidDf.show

      (validDf, invalidDf)
    } else {
      println("Df is empty")
      (spark.emptyDataFrame, spark.emptyDataFrame)
    }
  }

  private def transform(queries: List[String], df: DataFrame): DataFrame = {
    val strQ: String = queries.foldLeft("")((acc, el) => acc concat s"$el|")
    val otlQ: OTLQuery = OTLQuery(strQ)
    val Converter: Converter = new Converter(otlQ)
    Converter.setDF(df)
    val transformedDf: DataFrame = Converter.run

    transformedDf
  }

  private def getCommon(epochTime: Double): (ZonedDateTime, ZonedDateTime) = {
    val epoch: BigDecimal = BigDecimal(epochTime)
    val res: (BigDecimal, BigDecimal) = epoch /% BigDecimal(1)

    val seconds: Long = res._1.toLongExact
    val nanos: Long = res._2.toString.split("\\.")(1).toLong * 1000

    val zdt: ZonedDateTime = Instant.ofEpochSecond(seconds, nanos).atZone(ZoneId.of("Europe/Moscow"))

    val cronDefinition: CronDefinition = CronDefinitionBuilder.instanceDefinitionFor(QUARTZ)
    val parser: CronParser = new CronParser(cronDefinition)

    val exTime: ExecutionTime = ExecutionTime.forCron(parser.parse("0 0 0 1/10 * ?"))
    val prevEx: Optional[ZonedDateTime] = exTime.lastExecution(zdt)
    val nextEx: Optional[ZonedDateTime] = exTime.nextExecution(zdt)

    (prevEx.get, nextEx.get)
  }

  private def getYears(epochTime: Double): String = {
    val range: (ZonedDateTime, ZonedDateTime) = getCommon(epochTime)
    s"${range._1.getYear}-${range._2.getYear}"
  }

  private def getMonths(epochTime: Double): String = {
    val range: (ZonedDateTime, ZonedDateTime) = getCommon(epochTime)
    s"${range._1.getMonthValue}-${range._2.getMonthValue}"
  }

  private def getDays(epochTime: Double): String = {
    val range: (ZonedDateTime, ZonedDateTime) = getCommon(epochTime)
    s"${range._1.getDayOfMonth}-${range._2.getDayOfMonth}"
  }

  private def getTimeRange(epochTime: Double): String = {
    val range: (ZonedDateTime, ZonedDateTime) = getCommon(epochTime)
    s"${range._1.toEpochSecond}-${range._2.toEpochSecond}"
  }

  private def getYearsUdf: UserDefinedFunction = udf[String, Double](getYears)
  private def getMonthsUdf: UserDefinedFunction = udf[String, Double](getMonths)
  private def getDaysUdf: UserDefinedFunction = udf[String, Double](getDays)
  private def getTimeRangeUdf: UserDefinedFunction = udf[String, Double](getTimeRange)

  def addTimeColumns(df: DataFrame): DataFrame = {
    df
      .withColumn("_year", getYearsUdf(col("omds_ts")))
      .withColumn("_month", getMonthsUdf(col("omds_ts")))
      .withColumn("_day", getDaysUdf(col("omds_ts")))
      .withColumn("_time_range", getTimeRangeUdf(col("omds_ts")))
  }

  def writeValidTmp(df: DataFrame, path: String): Unit = {
    if (!df.isEmpty) {
      val finalPartList: List[String] = timeCol ++ partFields

      df
        // ? ask
//        .repartition(finalPartList.map(col) : _*)
        .write
        .partitionBy(finalPartList: _*)
        .mode("overwrite")
        .parquet(path)
    }
  }

  def writeInvalidTmp(df: DataFrame, path: String): Unit = {
    if (!df.isEmpty) {
      df
        .write
        .mode("overwrite")
        .parquet(path)
    }
  }

  def replaceDir(files: List[String], src: String, dest: String): List[Path] = {
    val splitNum: Int = src.length

    files
      .map(_.splitAt(splitNum)._2)
      .map(file => s"$dest$file")
      .map(file => Paths.get(file))
  }

  // ??? спарк создает файлы со случайными именами
  def renameExistFiles(paths: List[Path]): Unit = {
    val exists: List[Path] = paths.collect { case path if Files.exists(path) => path }

    if (exists.nonEmpty) {
      val rndGen: Random.type = scala.util.Random
      val rnd: String = rndGen.nextInt(1000).toString
      exists.foreach(path => Files.move(path, path.resolveSibling(s"$rnd${path.getFileName}")))
    }
  }

  case class jsonData(stage: Int, sourceFiles: List[String], resultingFiles: List[(String, String)])

  def runTransaction(src: List[String], res: List[(String, String)], logPath: String, mode: Int = 0, stage: Int = 0): Unit = {
    implicit val formats: DefaultFormats.type = DefaultFormats

    val stage_0: jsonData = jsonData(stage, src, res)
    val json_0: String = write(stage_0)

    val logFile: String = s"$logPath\\log.translog.json"

    val file: File = new File(logFile)
    file.getParentFile.mkdirs

    val jsonPath: Path = Paths.get(logFile)
    Files.write(jsonPath, json_0.getBytes)

    res.foreach { el =>
      new File(el._2).getParentFile.mkdirs
      Files.move(Paths.get(el._1), Paths.get(el._2))
    }

    val json_1: String = write(stage_0.copy(stage = 1))
    Files.write(jsonPath, json_1.getBytes)

    if (mode == 0) {
      val srcFiles: List[String] = getFilesList("D:\\res\\tempds\\source1")
      val archFiles: List[Path] = replaceDir(srcFiles ,"D:\\res\\tempds\\source1", "D:\\res\\arch_tempdata")
      val res: List[(Path, Path)] = srcFiles.map(file => Paths.get(file)).zip(archFiles)
      res.foreach(el => Files.move(el._1, el._2))
    } else if (mode == 1) {
      src.foreach(file => Files.delete(Paths.get(file)))
    }

    val json_3: String = write(stage_0.copy(stage = 3))
    Files.write(jsonPath, json_3.getBytes)

    Files.delete(jsonPath)
  }

  def startCheckUp(logPath: String): Unit = {
    val logFile: Path = Paths.get(s"$logPath\\log.translog.json")
    val logExist: Boolean = if (Files.exists(logFile)) true else false

    if (logExist) {
      println(logFile)

      implicit val formats: DefaultFormats.type = DefaultFormats
      val data: JValue = parse(logFile.toString)
      val jsonData: jsonData = data.extract[jsonData]

      val scrFiles: List[Path] = jsonData.resultingFiles.map(el => el._1).map(Paths.get(_))
      val resFiles: List[Path] = jsonData.resultingFiles.map(el => el._2).map(Paths.get(_))

      val scrFilesExists: Boolean = scrFiles.forall(Files.exists(_))
      val resFilesExist: Boolean = resFiles.forall(Files.exists(_))

      if (!resFilesExist) {
        if (scrFilesExists) {
          println("Transaction was interrupted")
          resFiles.foreach(file => Files.delete(file))
        } else {
          println("Critical incident. Call 911.")
        }
      }

      if (!scrFilesExists) {
        println("Transaction was success")
        Files.delete(logFile)
      } else {
        println("Transaction was interrupted")
        scrFiles.foreach(file => if (Files.exists(file)) Files.delete(file) )
      }
    }
  }

}

object DataProc {
  def apply(enrichQs: List[String], validateQs: List[String], tempDs: String, partFields: List[String]) =
    new DataProc(enrichQs: List[String], validateQs: List[String], tempDs: String, partFields: List[String])

  def getFilesList(startDir: String): List[String] = {
    val path: Path = FileSystems.getDefault.getPath(startDir)

    Files
      .walk(path)
      .iterator
      .asScala
      .filter(Files.isRegularFile(_))
      .map(_.toString)
      .toList
  }

  def appendExtension(startDir: String): Unit = {
    val path: Path = FileSystems.getDefault.getPath(startDir)

    Files
      .walk(path)
      .iterator
      .asScala
      .filter(path => Files.isRegularFile(path) && !path.endsWith(".parquet") && !path.startsWith("_SCHEMA"))
      .foreach(path => Files.move(path, path.resolveSibling(s"${path.getFileName}.parquet")))
  }

  def listTDS(tdsPath: String): DataFrame = {
    def listDirectories(folder: File, indent: Int, sb: StringBuilder): Unit = {
      sb.append(getIndentString(indent))
      sb.append("+--")
      sb.append(folder.getName)
      sb.append("/")
      sb.append("\n")

      folder.listFiles().collect {
        case file if file.isDirectory => listDirectories(file, indent + 1, sb)
        //        case file if file.isFile => listFiles(file, indent + 1, sb)
      }

      //      for {
      //        file <- folder.listFiles()
      //        if file.isDirectory
      //      } listDirectories(file, indent + 1, sb)
    }

    def listFiles(file: File, indent: Int, sb: StringBuilder): Unit = {
      sb.append(getIndentString(indent))
      sb.append("+--")
      sb.append(file.getName)
      sb.append("\n")
    }

    def getIndentString(indent: Int): String = {
      val sb: StringBuilder = new StringBuilder

//      (0 until indent).toList.foreach(sb.append("|  "))

      for {
        _ <- (0 until indent).toList
      } sb.append("|  ")
      sb.toString
    }

    val folder: File = new File(tdsPath)
    val resString: String = if (folder.isDirectory) {
      val indent: Int = 0
      val sb: StringBuilder = new StringBuilder
      listDirectories(folder, indent, sb)
      sb.toString
    } else ""

    if (resString.nonEmpty) {
      import spark.implicits._
      resString.split("\n").toList.toDF("path")
    } else spark.emptyDataFrame
  }

  def writeTDS(df: DataFrame, tDSPath: String, partFields: List[String]): DataFrame = {
    df
      .write
      .partitionBy(partFields: _*)
      .mode("overwrite")
      .parquet(tDSPath)

    spark.emptyDataFrame
  }

  // + root dir - D:\res\megion\test2, ~equals (600)
  def getSubDirs(startDir: String): List[String] = {
    val path: Path = FileSystems.getDefault.getPath(startDir)

        Files
          .walk(path)
          .iterator
          .asScala
          .filter(Files.isDirectory(_))
          .map(_.toFile.getAbsolutePath)
          .toList
  }

  def getArrayOfSubDirectories(directoryName: String): Array[String] = {
    val subDirs = new File(directoryName).listFiles.filter(_.isDirectory).map(_.getAbsolutePath)
    subDirs.map(getArrayOfSubDirectories).foldLeft(subDirs)((acc, isd) => acc ++ isd)
  }

  // equals, + 30% execution time
  def getEndFolders(startDir: String): List[String] = {
    val files: List[String] = getFilesList(startDir)
    val maxCount: Int = files.map(str => str.count(_ == '/')).max
    val endFiles: List[String] = files.filter(_.count(_ == '/') == maxCount)

    val endFolders: List[String] = endFiles
      .map(_.split("/").toList)
      .map(_.dropRight(1))
      .map(_.mkString("/"))
      .distinct
//      .toSet

    endFolders
  }

  def getArrayOfLeafsByArrayOfDirs(directories: Array[String]): Array[String] = {
    directories.map(dir => (dir, new File(dir).listFiles.count(_.isDirectory))).filter(_._2 == 0).map(_._1)
  }

  def getArrayOfLeafs(basePath: String, directoryName: String): Array[String] = {
    val dirs = getArrayOfSubDirectories(new File(basePath, directoryName).getAbsolutePath)
    val leafs = getArrayOfLeafsByArrayOfDirs(dirs)
    if (leafs.length == 0) Array(new File(basePath,directoryName).getAbsolutePath) else leafs
  }

  def mergeSchemas(endDirs: List[String]): StructType = {
    val ddlList: List[String] = endDirs
      .map(dir => s"$dir\\_schema.txt")
      .map { path2Schema =>
        val schemaFile: BufferedSource = Source.fromFile(path2Schema)
        val textDDL: String = schemaFile.mkString
        schemaFile.close

        textDDL
      }

    val structList: List[StructType] = ddlList.map(StructType.fromDDL)
    val structListHead: StructType = structList.head
    val structListTail: List[StructType] = structList.tail

    val mergedSchema = structListTail.foldLeft(structListHead)((accSchema, schema) => {
     schema.foldLeft(accSchema)((accSchema, field) => {
       if (!accSchema.contains(field)) {
         if (accSchema.fields.map(_.name).contains(field.name)) {
           throw new Exception(s"Field ${field.name} has several types.")
         }
         accSchema.add(field)
       } else {
         accSchema
       }
     })
    })

    mergedSchema
  }

  def mergeLeafSchemas(leafs: Array[String]): StructType = {
    val ddlStrings = leafs.map(_ + "/_SCHEMA").map(refFile(_).bufferedReader().readLine())

    val structSchemas = ddlStrings.map(StructType.fromDDL)

    val mergedSchema = structSchemas.tail.foldLeft(structSchemas.head)((mergedSchema, anotherSchema) => {
      anotherSchema.foldLeft(mergedSchema)((mergedSchema, field) => {
        if (!mergedSchema.contains(field)) {
          if (mergedSchema.fields.map(_.name).contains(field.name)) {
            throw new Exception(s"Field ${field.name} has several types.")
          }
          mergedSchema.add(field)
        } else {
          mergedSchema
        }
      }
      )
    })

    mergedSchema
  }

}