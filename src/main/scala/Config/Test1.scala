package Config

import com.typesafe.config.{Config, ConfigFactory}

import java.io.File
import java.util

object Test1 extends App {

    val defConf: Config = ConfigFactory.defaultApplication()
    val defConfMain: Config = defConf.getConfig("main")

    val transformerSpan: String = defConfMain.getString("transformerSpan")
    println(transformerSpan)

    val partitionFields: util.List[AnyRef] = defConfMain.getList("partitionFields").unwrapped()
    // how transform ???
//    partitionFields.forEach(x => print(x.formatted(s"$x ")))

    val userConfFile: File = new File(defConfMain.getString("userConfigurationPath"))
    val userConfig: Config = ConfigFactory.parseFile(userConfFile)
    println(s"\n$userConfig")

    val finalConf: Config = userConfig.withFallback(defConf)
    println(finalConf)

    println(finalConf.getConfig("main").getString("tdsTimeSpan"))
    println(finalConf.getConfig("main").getConfig("validateQueries").getString("validate1"))


}
