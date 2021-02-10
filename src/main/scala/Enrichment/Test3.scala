package Enrichment

import Config.ConfigProc
import DataProc.listTDS

object Test3 extends App {

  val conf: ConfigProc = ConfigProc("D:\\scala_work\\ETL_conc\\src\\main\\resources\\application.conf")

  val eQL: List[String] = List("eval x = 10", "eval y = 20")
  val vQL: List[String] = List("""filter {"query": "(x=\"10\")", "fields": ["x"]}""")

  val src: String = conf.tempDsPath.getOrElse("")
  val partFld = conf.partitionFields.getOrElse(List())


  val testProc = DataProc(eQL, vQL, src, partFld)
  listTDS("D:\\res\\tds").show(truncate = false)





}
