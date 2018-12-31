import java.util.concurrent.atomic.AtomicInteger

import scala.collection.mutable

object SegList {
  //variable without new what does it mean
  private val segments = new mutable.MutableList[Segment]
  private val fileLimit = 1024 //1mb
  private val fileId = new AtomicInteger(0)
  private val logFolder = "./log-files"

  def put(key: String, value: String): Unit = {
    if (segments.isEmpty || segments.last.size > fileLimit) {
      val curFileName = s"$logFolder/${fileId.incrementAndGet()}.log"
      segments += new Segment(curFileName)
    }
    val curSegment = segments.last
    curSegment.add(key, value)
  }

  //lock
  def get(key: String): Option[String] = segments.view.reverse.map(_.get(key)).find(_.isDefined).flatten

  //lock
  def remove(key: String): Unit = {
    segments.foreach(_.remove(key))
  }
}
