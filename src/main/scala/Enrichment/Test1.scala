package Enrichment

import Config.ConfigProc
import Enrichment.DataProc.getFilesList
import org.apache.spark.sql.DataFrame

object Test1 extends App {

  val conf: ConfigProc = ConfigProc("D:\\scala_work\\ETL_conc\\src\\main\\resources\\application.conf")

  val eQL: List[String] = List("eval x = 10", "eval y = 20")
  val vQL: List[String] = List("""filter {"query": "(x=\"10\")", "fields": ["x"]}""")

  val src: String = conf.tempDsPath.getOrElse("")
  val partFld = conf.partitionFields.getOrElse(List())
//  println(partFld)

  val testProc = DataProc(eQL, vQL, src, partFld)

  val resDfs: (DataFrame, DataFrame) = testProc.process()
//  resDfs._1.show
//  resDfs._2.show

  val goodDfwTFld = testProc.addTimeColumns(resDfs._1)
  goodDfwTFld.show

  val tmpValid: String = conf.tempTdsValidPath.getOrElse("")
//  println(tmpValid)
  testProc.writeValidTmp(goodDfwTFld, tmpValid)

  val badDfwTFld = testProc.addTimeColumns(resDfs._2)
  badDfwTFld.show

  val tmpInvalid: String = conf.tempTdsInvalidPath.getOrElse("")
  testProc.writeInvalidTmp(badDfwTFld, tmpInvalid)

  val validFiles: List[String] = getFilesList("D:\\res\\temp_valid")
  val invalidFiles = getFilesList("D:\\res\\temp_invalid")

//  println(validFiles)
//  println(invalidFiles)

  val tdsValid = testProc.replaceDir(validFiles, "D:\\res\\temp_valid","D:\\res\\tds")
//  println(tdsValid)
  val tdsInvalid = testProc.replaceDir(invalidFiles, "D:\\res\\temp_invalid" ,"D:\\res\\dumpdata")
//  println(tdsInvalid)
//
  val tdsFinal = tdsValid ++ tdsInvalid
//  println(tdsFinal)

  testProc.renameExistFiles(tdsFinal)
//
  val tempDSFiles = getFilesList("D:\\res\\tempds\\source1")

  val tmpFinal = validFiles ++ invalidFiles
  val tdsFinalStr = tdsFinal.map(_.toString)
  val pairs = tmpFinal.zip(tdsFinalStr)
//  println(pairs)

  testProc.runTransaction(tempDSFiles, pairs , "D:\\res\\translog")





  val tDSAll = getFilesList("D:\\res\\tds")
  println(tDSAll)





}
