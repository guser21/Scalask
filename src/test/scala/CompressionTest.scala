import com.scalask.compression.LastTwoMerger
import com.scalask.model.SegmentList
import org.scalatest.{BeforeAndAfterEach, FunSuite}

import scala.io.Source
import scala.sys.process._

class CompressionTest extends FunSuite with BeforeAndAfterEach {
  val testFolder = "./log-files-test"

  override def beforeEach(): Unit = {
    super.beforeEach()
    s"rm -rf $testFolder" !
  }

  test("Compression test") {
    val segList = new SegmentList(testFolder, (_: SegmentList) => Unit, 1024)
    (1 to 10000).foreach {
      case i if i % 3 == 0 => segList.put("0", s"$i")
      case i if i % 3 == 1 => segList.put("1", s"$i")
      case i if i % 3 == 2 => segList.remove("0")
    }
    LastTwoMerger.compress(segList)
    assertResult(Source.fromFile(s"$testFolder/2.log").getLines().size)(2)
  }
}
