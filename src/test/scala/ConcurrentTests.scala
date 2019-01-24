import com.scalask.compression.{LastTwoMerger, WheneverPossibleScheduler}
import com.scalask.model.SegmentList
import org.scalatest.{BeforeAndAfterEach, FunSuite}

import scala.sys.process._

class ConcurrentTests extends FunSuite with BeforeAndAfterEach {
  val testFolder = "./log-files-test"

  override def beforeEach() {
    s"rm -rf $testFolder" !
  }


  test("Concurrent put and get") {
    val segList = new SegmentList(testFolder, new WheneverPossibleScheduler(LastTwoMerger), 1024 * 1024)
    (1 to 100000).par.foreach {
      case i if i % 2 == 0 => segList.put(i.toString, s"val-$i")
      case i if i % 2 == 1 && i > 50 => segList.get((i - 41).toString)
        .foreach(e => if (e != s"val-${i - 41}") throw new RuntimeException(s"key: ${i - 41} val: $e"))
      case _ => Unit
    }

  }

  test("Concurrent put, get, delete - limited keys") {
    val segList = new SegmentList(testFolder, new WheneverPossibleScheduler(LastTwoMerger), 1024)
    putDeleteRemove(segList)
  }

  test("Concurrent put, get, delete - limited keys && no merge") {
    val segList = new SegmentList(testFolder, (_: SegmentList) => Unit, 1024)
    putDeleteRemove(segList)
  }

  def putDeleteRemove(segList: SegmentList, times: Int = 1000 * 100) = (1 to times).par.foreach {
    case i if i % 3 == 0 => segList.put("0", s"$i")
    case i if i % 3 == 1 => segList.get("0")
    case i if i % 3 == 2 => segList.remove("0")
  }

}
