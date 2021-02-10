import Enrichment.ListTDS
import ot.dispatcher.sdk.core.SimpleQuery
import ot.dispatcher.sdk.test.CommandTest

class ListTDSTest extends CommandTest {
  override val dataset: String = "[]"

  import spark.implicits._

  test("Test 1. Command: listtds with non-empty arguments list") {
    val tDSpath = utils.mainConfig.getConfig("main").getString("tdsPath")
    val query = SimpleQuery(s"path=$tDSpath")
    val command = new ListTDS(query, utils)
    val actual = execute(command)
    val expected = Seq(
      "+--tds/",
      "|  +--_year=2020-2020/",
      "|  |  +--_month=12-12/",
      "|  |  |  +--_day=11-21/",
      "|  |  |  |  +--_time_range=1607634000-1608498000/",
      "|  |  |  |  |  +--x=10/",
      "|  |  |  |  |  |  +--y=20/"
    ).toDF("path").toJSON.collect().mkString("[\n",",\n","\n]")
    assert(jsonCompare(actual, expected), f"Result : $actual\n--\nExpected : $expected")
  }

  test("Test 2. Command: listtds with empty arguments list") {
    val query = SimpleQuery("")
    val command = new ListTDS(query, utils)
    val actual = execute(command)
    val expected = Seq(
      "+--tds/",
      "|  +--_year=2020-2020/",
      "|  |  +--_month=12-12/",
      "|  |  |  +--_day=11-21/",
      "|  |  |  |  +--_time_range=1607634000-1608498000/",
      "|  |  |  |  |  +--x=10/",
      "|  |  |  |  |  |  +--y=20/"
    ).toDF("path").toJSON.collect().mkString("[\n",",\n","\n]")
    assert(jsonCompare(actual, expected), f"Result : $actual\n--\nExpected : $expected")
  }
}
