import com.scalask.compression.{LastTwoMerger, WheneverPossibleScheduler}
import com.scalask.model.SegmentList
import org.scalatest.{BeforeAndAfterAll, FunSuite}

import scala.sys.process._
import scala.util.Random

class EntryTest extends FunSuite with BeforeAndAfterAll {
  private val testFolder = "./log-files-test"

  override def beforeAll() {
    s"rm -rf ./$testFolder" !
  }

  test("Full Test") {

    val segList = new SegmentList(testFolder, new WheneverPossibleScheduler(LastTwoMerger), 1024)
    (1 to 10000).foreach(e => segList.put(e.toString, e.toString))
    (5000 to 10000).foreach(e => segList.put(e.toString, (-e).toString))

    val wrongUpdate = (1 to 400).map(_ => Random.nextInt(10000))
      .map(_.toString)
      .map(segList.get)
      .filter(_.isDefined)
      .flatten
      .map(_.toInt)
      .find(x => !((Math.abs(x) < 5000 && x < 5000 && x >= 0) || (Math.abs(x) >= 5000 && x < 0)))
    assert(wrongUpdate.isEmpty)

    assertResult(segList.get("423"))(Some("423"))
    assertResult(segList.get("7777"))(Some("-7777"))
    (200 to 4000).foreach(x => segList.remove(x.toString))

    val removal = (200 to 4000).find(x => segList.get(x.toString).isDefined)
    if (removal.isDefined) throw new RuntimeException("Removal doesn't work " + removal)

    assertResult(segList.get("32445"))(None)
  }


}
