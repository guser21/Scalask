import org.scalatest.{BeforeAndAfterAll, FunSuite}

import scala.sys.process._
import scala.util.Random

class EntryTest extends FunSuite with BeforeAndAfterAll {
  override def beforeAll() {
    "rm -rf ./log-files" !
  }

  test("Full Test") {
    val bitcask: DB = new Bitcask
    (1 to 10000).foreach(e => bitcask.put(e.toString, e.toString))
    (5000 to 10000).foreach(e => bitcask.put(e.toString, (-e).toString))

    val wrongUpdate = (1 to 400).map(_ => Random.nextInt(10000))
      .map(_.toString)
      .map(bitcask.get)
      .filter(_.isDefined)
      .flatten
      .map(_.toInt)
      .find(x => !((Math.abs(x) < 5000 && x < 5000 && x >= 0) || (Math.abs(x) >= 5000 && x < 0)))
    assert(wrongUpdate.isEmpty)

    assertResult(bitcask.get("423"))(Some("423"))
    assertResult(bitcask.get("7777"))(Some("-7777"))
    (200 to 4000).foreach(x => bitcask.remove(x.toString))

    val removal = (200 to 4000).find(x => bitcask.get(x.toString).isDefined)
    if (removal.isDefined) throw new RuntimeException("Removal doesn't work " + removal)

    assertResult(bitcask.get("32445"))(None)
  }


}
