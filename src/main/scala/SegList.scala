import java.io.File
import java.nio.file.{Files, Paths}
import java.util.concurrent.atomic.AtomicInteger

import scala.collection.mutable

class SegList(logFolder: String) {
  private val segments = new mutable.ListBuffer[Segment]
  private val fileLimit = 1024 //1kb
  private val fileId = new AtomicInteger(0)

  if (!Files.exists(Paths.get(logFolder))) Files.createDirectory(Paths.get(logFolder))

  //should be locked
  new File(logFolder)
    .listFiles()
    .map(_.getName)
    .map(e => e.takeWhile(c => c >= '0' && c <= '9').foldLeft(0)((cur, c) => cur * 10 + (c - '0'))) //name to id
    .sorted
    .map(id => new Segment(id, logFolder + id + ".log"))
    .foreach(seg => {
      //absolutely non readable
      seg.load().foreach(key => segments.foreach(cs => cs.remove(key)))
      segments += seg
    })

  def put(key: String, value: String): Unit = availableSegment().add(key, value)

  //lock
  def get(key: String): Option[String] = {
    segments.view.reverse.map(_.get(key)).find(_.isDefined).flatten
  }

  //lock
  def remove(key: String): Unit = {
    segments.foreach(_.remove(key))
    availableSegment().setDeleteFlag(key)
  }

  def availableSegment(): Segment = {
    if (segments.isEmpty || segments.last.size > fileLimit) {
      val id = fileId.incrementAndGet()
      val curFileName = s"$logFolder/$id.log"
      segments += new Segment(id, curFileName)
    }
    segments.last
  }
}
