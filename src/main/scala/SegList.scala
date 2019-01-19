import java.io.File
import java.nio.file.{Files, Paths}
import java.util.concurrent.locks.ReentrantReadWriteLock

import scala.collection.mutable
import scala.util.{Failure, Success, Try}

class SegList(logFolder: String, scheduler: MergeScheduler, fileLimit: Int = 1024) {
  val segments = new mutable.ListBuffer[Segment]
  private var fileId = 0

  val segmentListLock = new ReentrantReadWriteLock()
  val segmentListReadLock: ReentrantReadWriteLock.ReadLock = segmentListLock.readLock()
  val segmentListWriteLock: ReentrantReadWriteLock.WriteLock = segmentListLock.writeLock()

  if (!Files.exists(Paths.get(logFolder))) Files.createDirectory(Paths.get(logFolder))


  acquireSegListWriteLock({
    new File(logFolder)
      .listFiles()
      .map(_.getName)
      .map(e => e.takeWhile(c => c >= '0' && c <= '9').foldLeft(0)((cur, c) => cur * 10 + (c - '0'))) //name to id
      .sorted
      .map(id => new Segment(id, logFolder))
      .foreach(seg => {
        seg.load().foreach(key => segments.foreach(cs => cs.removeFromIndex(key)))
        segments += seg
      })

    fileId = new File(logFolder)
      .listFiles()
      .map(_.getName)
      .map(e => e.takeWhile(c => c >= '0' && c <= '9').foldLeft(0)((cur, c) => cur * 10 + (c - '0'))) //name to id
      .foldLeft(0)((cur, e) => Math.max(cur, e))

    //as the read segments are "dead"- we dont add elements to segments of previous sessions
    //we add new empty segment for the new entries to be added to a clean segment
    addSegment()
  })

  def put(key: String, value: String): Unit = acquireSegListReadLock(availableSegment().add(key, value))


  def get(key: String): Option[String] = acquireSegListReadLock({
    segments.view.reverse.map(_.get(key)).find(_ != NoInfo()).getOrElse(NotInDatabase()) match {
      case NotInDatabase() => None
      case HasValue(value) => Option(value)
      case NoInfo() => throw new IllegalStateException("Should have been filtered")
    }
  })


  def remove(key: String): Unit = acquireSegListReadLock(availableSegment().setDeleteFlag(key))

  private def acquireSegListReadLock[T](criticalSection: => T): T = {
    segmentListReadLock.lock()
    Try {
      criticalSection
    } match {
      case Failure(exception) => segmentListReadLock.unlock(); throw exception
      case Success(res) => segmentListReadLock.unlock(); res
    }
  }

  private def acquireSegListWriteLock[T](criticalSection: => T): T = {
    segmentListWriteLock.lock()
    Try {
      criticalSection
    } match {
      case Failure(exception) => segmentListWriteLock.unlock(); throw exception
      case Success(res) => segmentListWriteLock.unlock(); res
    }
  }

  private def availableSegment(): Segment = {
    if (segments.isEmpty || segments.last.size > fileLimit) {
      segmentListReadLock.unlock()
      segmentListWriteLock.lock()
      Try {
        //double check as between lock the state might have changed
        if (segments.isEmpty || segments.last.size > fileLimit) {
          addSegment()
          scheduler.notifySegmentAdded(this)
        }
      } match {
        //in case of success reacquire read lock, as it has been hold
        case Success(_) => segmentListWriteLock.unlock(); segmentListReadLock.lock()
        //just release all locks
        case Failure(exception) => segmentListWriteLock.unlock(); throw exception
      }
    }
    segments.last
  }

  private def addSegment(): Unit = acquireSegListWriteLock({
    fileId = fileId + 1
    val id = fileId
    val curSegment = new Segment(id, logFolder)
    segments += curSegment
  })
}
