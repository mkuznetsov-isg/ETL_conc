package Enrichment

import Enrichment.DataProc.getFilesList
import TempDS.SparkSessionWrapper
import org.apache.spark.sql.types.StructType

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}
import scala.io.Source


object SchemaAppender extends App with SparkSessionWrapper {

  val files: List[String] = getFilesList("D:\\res\\home\\andrey\\tmp\\test\\xyz\\part-C.parquet")
  println(s"${files.size}")
  println(s"$files")

  val maxCount: Int = files.map(str => str.count(_ == '\\')).max
  println(maxCount)

  val endFiles: List[String] = files.filter(_.count(_ == '\\') == maxCount)
  println(s"$endFiles\n")

  val folders: List[String] = endFiles
    .map(_.split("\\\\").toList)
    .map(_.dropRight(1))
    .map(_.mkString("\\"))
    .distinct
  println(s"$folders\n")

//  val path = "D:\\res\\megion\\test\\_time_range=1572555600000-1572642000000\\_year=2019\\_month=11\\_day=01\\_department_num=4\\_deposit=Травяное\\_pad_num=29б\\_object=скважина_655"

  folders.foreach { folder =>
    val schema = spark
      .read
      .parquet(folder)
      .schema
      .toDDL

    println(s"$schema\n")

    Files.write(Paths.get(s"$folder\\_SCHEMA"), schema.getBytes(StandardCharsets.UTF_8))
  }

//  val srcFile = Source
//    .fromFile("D:\\res\\megion\\test\\_time_range=1572555600000-1572642000000\\_year=2019\\_month=11\\_day=01\\_department_num=4\\_deposit=Травяное\\_pad_num=29б\\_object=скважина_655\\_schema.txt")
//  val textDDL = srcFile.mkString
//  srcFile.close
//
//  println(s"$textDDL\n")


//  val data2 = spark
//    .read
//    .schema(textDDL)
//    .parquet(path)
//
//  data2.show


//  val data = spark
//    .read
//    .option("basePath", "D:\\res\\megion\\test\\")
//    .parquet(s"$path\\*.parquet")
//    .parquet(path)

//  data.show()



//  val strSchema = schema.toString
//  println(s"$strSchema\n")
//
//  val parsedSchema = strSchema
//    .slice(11, strSchema.length - 1)
//    .replace("StructField","")
//    .replace("),", ")")
//    .split(" ")
//    .map(_.replace("(", ""))
//    .map(_.split(",").dropRight(1))
//    .map(el => s"${el(0)}: ${el(1)}")
//    .mkString(", ")
//
//  println(parsedSchema)
//
//  Files.write(Paths.get(s"$path\\_schema.txt"), parsedSchema.getBytes(StandardCharsets.UTF_8))


}
