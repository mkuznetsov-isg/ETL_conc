import Enrichment.WriteTDS
import ot.dispatcher.sdk.core.SimpleQuery
import ot.dispatcher.sdk.test.CommandTest
import scala.collection.JavaConverters._

class WriteTDSTest extends CommandTest {
  override val dataset: String = "[]"

  test("Test 1. Command: writetds with non-empty arguments list") {
    val tDSpath: String = utils.mainConfig.getConfig("main").getString("tdsPath")
    val partFields: String =
      utils.mainConfig
        .getConfig("main")
        .getList("partitionFields")
        .unwrapped()
        .asScala
        .mkString(",")

    val query = SimpleQuery(s"path=$tDSpath pfields=$partFields")
    val command = new WriteTDS(query, utils)
    val actual = execute(command)
//    val expected = """[{"omds_uid": "first", "omds_hash": 2}, {"omds_uid": "second", "omds_hash": 4}]"""
//    assert(jsonCompare(actual, expected), f"Result : $actual\n--\nExpected : $expected")
  }

  test("Test 2. Command: writetds with empty arguments list") {
    val query = SimpleQuery("")
    val command = new WriteTDS(query, utils)
    val actual = execute(command)
    //    val expected = """[{"omds_uid": "first", "omds_hash": 2}, {"omds_uid": "second", "omds_hash": 4}]"""
    //    assert(jsonCompare(actual, expected), f"Result : $actual\n--\nExpected : $expected")
  }
}
