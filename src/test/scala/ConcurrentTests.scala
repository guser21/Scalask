import org.scalatest.{BeforeAndAfterEach, FunSuite}

import scala.sys.process._

class ConcurrentTests extends FunSuite with BeforeAndAfterEach {
  override def beforeEach() {
    "rm -rf ./log-files" !
  }

    test("Concurrent put and get") {
      val scalask = new Scalask
      (1 to 1000000).par.foreach {
        case i if i % 2 == 0 => scalask.put(i.toString, s"val-$i")
        case i if i % 2 == 1 && i > 50 => scalask.get((i - 41).toString)
          .foreach(e => if (e != s"val-${i - 41}") throw new RuntimeException(s"key: ${i - 41} val: $e"))
        case _ => Unit
      }
    }

  test("Concurrent put, get, delete - limited keys") {
    val scalask = new Scalask
    (1 to 100000).foreach {
      case i if i % 3 == 0 => scalask.put("0", s"$i")
      case i if i % 3 == 1 => scalask.get("0")
      case i if i % 3 == 2 => scalask.remove("0")
    }
  }
}
