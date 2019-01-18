import org.scalatest.{BeforeAndAfterAll, FunSuite}

import scala.sys.process._

class ConcurrentTests extends FunSuite with BeforeAndAfterAll {
  override def beforeAll() {
    "rm -rf ./log-files" !
  }

  test("Concurrent put and get") {
    val scalask = new Scalask
    (1 to 1000000).par.foreach {
      case i if i % 2 == 0 => scalask.put(i.toString, s"val-$i")
      case i if i % 2 == 1 && i> 50=> scalask.get((i-41).toString)
        .foreach(e => if (e != s"val-${i-41}") throw new RuntimeException(s"key: ${i-41} val: $e"))
      case _ => Unit
    }
  }

}
