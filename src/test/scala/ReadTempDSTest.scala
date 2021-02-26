import TempDS.TempDSReader
import ot.dispatcher.sdk.core.SimpleQuery
import ot.dispatcher.sdk.test.CommandTest

class ReadTempDSTest extends CommandTest {
  override val dataset: String = "[]"

  test("Test 1. Command: readtempds") {
    val tempDSpath = utils.mainConfig.getConfig("main").getString("tempDsPath")
    val query = SimpleQuery(s"path=$tempDSpath")
    val command = new TempDSReader(query, utils)
    val actual = execute(command)
    val expected = """[{"omds_uid": "first", "omds_hash": 2}, {"omds_uid": "second", "omds_hash": 4}]"""
    assert(jsonCompare(actual, expected), f"Result : $actual\n--\nExpected : $expected")
  }
}
