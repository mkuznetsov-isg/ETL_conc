package Config

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.logging.log4j.{LogManager, Logger}

import java.io.File
import java.util
import scala.collection.JavaConverters._
import scala.util.{Failure, Success, Try}

class ConfigProc(pathToConfig: String) {

  private val log: Logger = LogManager.getLogger("Some Logger")
  log.info("logger started")

  private val defConf: Try[Config] = loadConf(pathToConfig)
  private val paramsIsValid: Boolean = checkParams(defConf)
  private val userConf: Try[Config] = loadConf(defConf)

  private val mergedConf: Option[Config] = if (defConf.isSuccess && paramsIsValid && userConf.isSuccess) {
    val mergedConf: Config = userConf.get.withFallback(defConf.get)
//    println(mergedConf)
    Option(mergedConf)
    } else None

  log.info(mergedConf)

  // mergedConf.get
  val transformerSpan: Try[String] = Try { mergedConf.get.getString("transformerSpan") }
  val storage: Try[String] = Try { mergedConf.get.getString("storage") }
  val sourceType: Try[String] = Try { mergedConf.get.getString("sourceType") }
  val tempDsPath: Try[String] = Try { mergedConf.get.getString("tempDsPath") }
  val tempTdsValidPath: Try[String] = Try { mergedConf.get.getString("tempTdsValidPath") }
  val tempTdsInvalidPath: Try[String] = Try { mergedConf.get.getString("tempTdsInvalidPath") }
  val tdsPath: Try[String] = Try { mergedConf.get.getString("tdsPath") }
  val dumpDataPath: Try[String] = Try { mergedConf.get.getString("dumpDataPath") }
  val tdsTimeSpan: Try[String] = Try { mergedConf.get.getString("tdsTimeSpan") }
  val partitionFields: Try[List[String]] = Try {
    mergedConf.get.getList("partitionFields").unwrapped().asScala.toList.asInstanceOf[List[String]]
  }
  val enrichQueries = Try { mergedConf.get.getConfig("enrichQueries").entrySet }

  val validateQueries = Try { mergedConf.get.getConfig("validateQueries").entrySet }


  private def loadConf(path: String): Try[Config] =
    Try {
      val defConfFile: File = new File(path)
      val defConfig: Config = ConfigFactory.parseFile(defConfFile).getConfig("main")
      defConfig
    }

  private def loadConf(conf: Try[Config]): Try[Config] =
    Try {
      val userConfFile: File = new File(conf.get.getString("userConfigurationPath"))
      val userConfig: Config = ConfigFactory.parseFile(userConfFile).getConfig("main")
      userConfig
    }

  private def checkParams(conf: Try[Config]): Boolean = {
    def check(arr: Array[String]): Try[Boolean] =
      arr match {
        case Array(name, paramType) if paramType == "S" => Try { conf.get.getString(name).isEmpty }
        case Array(name, paramType) if paramType == "L" => Try { conf.get.getList(name).isEmpty }
        case Array(_, paramType) if paramType == "EQ" => Try { conf.get.getConfig("enrichQueries").isEmpty }
        case Array(_, paramType) if paramType == "VQ" => Try { conf.get.getConfig("validateQueries").isEmpty }
      }

    def checkRequiredParams(list: util.List[AnyRef]): Boolean = {
      val sList: List[Try[Boolean]] =
        list
          .asScala
          .toList
          .asInstanceOf[List[String]]
          .map(_.split("_"))
          .map(check)

      sList.contains(Try(true)) || sList.exists(_.isFailure)
    }

    val checkListExist: Try[util.List[AnyRef]] = Try { conf.get.getList("requiredParams").unwrapped() }

    val res: Boolean = checkListExist match {
      case Success(v) => checkRequiredParams(v)
      case Failure(_) => true
    }

    !res
  }

//  private def loadUserConf(conf: Try[Config]): Try[Config] =
//    Try {
//      val userConfFile: File = new File(conf.get.getString("userConfigurationPath"))
//      val userConfig: Config = ConfigFactory.parseFile(userConfFile).getConfig("main")
//      userConfig
//    }

}

object ConfigProc {
  def apply(pathToConfig: String): ConfigProc = new ConfigProc(pathToConfig)
}
