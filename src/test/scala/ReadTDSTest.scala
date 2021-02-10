
import Enrichment.ReadTDS_old_4
import ot.dispatcher.sdk.core.SimpleQuery
import ot.dispatcher.sdk.test.CommandTest

import scala.collection.JavaConverters._

class ReadTDSTest extends CommandTest {
  override val dataset: String = "[]"

  test("Test 1. Command: readtds with non-empty arguments list") {
//    val tDSpath: String = utils.mainConfig.getConfig("main").getString("tdsPath")
//    val partFields: String =
//      utils.mainConfig
//        .getConfig("main")
//        .getList("partitionFields")
//        .unwrapped()
//        .asScala
//        .mkString(",")

    val query = SimpleQuery(s" ds=tds address=${"\""}/part1=1/part2=${"\""}*${"\""}/part3=${"\""}1, 2${"\""}/part4=*/${"**"} field1>=1 and field1<=10${"\""} tws=1573257600 twf=1573344000000 metrics=${"\""}adkuVrbPcollector,  adkuVrbPcollector_period${"\""}")
//    val query = SimpleQuery(s" ds=tds address=${"\""}/part1=1/part2=${"\""}*${"\""}/part3=${"\""}1, 2${"\""}/part4=*/${"**"} field1>=1 and field1<=10${"\""} metrics=${"\""}adkuVrbPcollector,  adkuVrbPcollector_period${"\""}")

//    val query = SimpleQuery(s" ds=tds _deposit=${"\""}Тайлаковс*${"\""} _pad_num=* adkuVrbPcollector>197 adkuVrbPcollector<198  metrics=${"\""}adkuVrbPcollector,adkuVrbPcollector_period${"\""}")
//    val query = SimpleQuery(s" ds=tds _deposit=${"\""}Тайлаковское${"\""} _pad_num=4 _object=скважина_4 _year=2019 _month=11 _day=${"\""}9,10${"\""} metrics=${"\""}adkuVrbPcollector,  adkuVrbPcollector_period${"\""}")
//    val query = SimpleQuery(s" ds=tds _deposit=${"\""}Тайлаковс*${"\""} _pad_num=* adkuVrbPcollector=${"\""}197.6,  197.4${"\""} metrics=${"\""}adkuVrbPcollector,  adkuVrbPcollector_period${"\""}")
//    val query = SimpleQuery(s" _deposit=${"\""}Тайлаковское${"\""} _pad_num=* metrics=*")

//    val query = SimpleQuery(s"ds=${"\""}tds, fds${"\""} _sourcename=adku _deposit=${"\""}Тайлаковское${"\""} _pad_num=* _well_num=${"\""}1,14,15,16,256р${"\""} metrics=${"\""}adkuWellInputP, adkuWellZatrubP, adkuWellStatus, adkuWellLiquidDebit, adkuWellInputT, adkuWellBufferP, adkuWellUstT${"\""}")
//    val query = SimpleQuery(s"ds=${"\""}tds, fds${"\""} _sourcename=adku _deposit=${"\""}Тайлаковское${"\""} _pad_num=* _object=${"\""}скважина*${"\""} _num=${"\""}1,14,15,16,256р${"\""} metrics=${"\""}adkuWellInputP, adkuWellZatrubP, adkuWellStatus, adkuWellLiquidDebit, adkuWellInputT, adkuWellBufferP, adkuWellUstT${"\""}")
//    val query = SimpleQuery(s"_storage=fond ds=fds _deposit=${"\""}Тайлаковское${"\""} _well_num=${"\""}256р${"\""} metrics=${"\""}adkuWellInputP, adkuWellZatrubP, adkuWellStatus, adkuWellLiquidDebit, adkuWellInputT, adkuWellBufferP, adkuWellUstT, oilWellopAverageLiquidDebit, oilWellopVolumeWater${"\""}")
//    val query = SimpleQuery(s"_storage=fond ds=fds _deposit=${"\""}Тайлаковское${"\""} _pad_num=4 _well_num=* metrics=${"\""}adkuWellInputP, adkuWellZatrubP, adkuWellStatus, adkuWellLiquidDebit, adkuWellInputT, adkuWellBufferP, adkuWellUstT, oilWellopAverageLiquidDebit, oilWellopVolumeWater${"\""}")
//    val query = SimpleQuery(s"_deposit=${"\""}Тайлаковское${"\""} _well_num=${"\""}256р${"\""} metrics=* tws=1610485200 twf=1610571600")
//    val query = SimpleQuery(s"_deposit=${"\""}Тайлаковское${"\""} _well_num=${"\""}256р${"\""} metrics=${"\""}adkuWellInputP, adkuWellZatrubP, adkuWellStatus, adkuWellLiquidDebit, adkuWellInputT, adkuWellBufferP, adkuWellUstT, oilWellopAverageLiquidDebit, oilWellopVolumeWater, perf${"\""}")
//    val query = SimpleQuery(s"_deposit=${"\""}Тайлaковское${"\""} _well_num=${"\""}256р${"\""} actual_time=false _real_time=* metrics=${"\""}adkuWellInputP, adkuWellStatus, oilWellopAverageLiquidDebit, oilWellopVolumeWater${"\""}")
//    val query = SimpleQuery(s"_deposit=${"\""}Тайлаковское${"\""} _well_num=${"\""}256р${"\""} _real_time=* actual_time=true metrics=${"\""}adkuWellInputP, adkuWellStatus, oilWellopAverageLiquidDebit, oilWellopVolumeWater${"\""}")
//    val query = SimpleQuery(s"arg1>=111 arg2<=222  arg3!=${"\""}3,33,333${"\""}   arg4=444    arg5>5 arg5<6 arg6=some1* arg7!=some2* tws=*")
//    val query = SimpleQuery(s"arg1>=111")
//    val query = SimpleQuery(s"arg3!=${"\""}3,33,333${"\""}")
//    val query = SimpleQuery(s"arg6=some1*")
//    val query = SimpleQuery(s"arg7!=some2*")
//    val query = SimpleQuery(s"tws=*")
//    val query = SimpleQuery(s"arg3=${"\""}3,33,333${"\""}")
    val command = new ReadTDS_old_4(query, utils)
    val actual = execute(command)
//    val expected = """[{"omds_uid": "first", "omds_hash": 2}, {"omds_uid": "second", "omds_hash": 4}]"""
//    assert(jsonCompare(actual, expected), f"Result : $actual\n--\nExpected : $expected")
  }

//  test("Test 2. Command: writetds with empty arguments list") {
//    val query = SimpleQuery("")
//    val command = new WriteTDS(query, utils)
//    val actual = execute(command)
    //    val expected = """[{"omds_uid": "first", "omds_hash": 2}, {"omds_uid": "second", "omds_hash": 4}]"""
    //    assert(jsonCompare(actual, expected), f"Result : $actual\n--\nExpected : $expected")
//  }
}
