package Config

import com.typesafe.config.ConfigValue

import java.util
import java.util.Map

object Test2 extends App {

  val conf: ConfigProc = ConfigProc("D:\\scala_work\\ETL_conc\\src\\main\\resources\\application.conf")

  val eq = conf.enrichQueries.get
  val vq = conf.validateQueries.get

  println(eq)
  println(vq)

}