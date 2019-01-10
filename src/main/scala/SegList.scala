import java.io.File
import java.nio.file.{Files, Paths}
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.locks.ReentrantReadWriteLock

import scala.collection.mutable
import scala.util.{Failure, Success, Try}

class SegList(logFolder: String, scheduler: MergeScheduler) {

  val segments = new mutable.ListBuffer[Segment]
  private val fileLimit = 1024 //1kb
  private val fileId = new AtomicInteger(0)
  val segmentListLock = new ReentrantReadWriteLock()
  val segmentListReadLock: ReentrantReadWriteLock.ReadLock = segmentListLock.readLock()
  val segmentListWriteLock: ReentrantReadWriteLock.WriteLock = segmentListLock.writeLock()

  if (!Files.exists(Paths.get(logFolder))) Files.createDirectory(Paths.get(logFolder))


  // TODO extract to function
  segmentListWriteLock.lock()
  Try({
    new File(logFolder)
      .listFiles()
      .map(_.getName)
      .map(e => e.takeWhile(c => c >= '0' && c <= '9').foldLeft(0)((cur, c) => cur * 10 + (c - '0'))) //name to id
      .sorted
      .map(id => new Segment(id, logFolder + id + ".log"))
      .foreach(seg => {
        //absolutely non readable
        seg.load().foreach(key => segments.foreach(cs => cs.removeFromIndex(key)))
        segments += seg
      })
  }) match {
    case Failure(exception) => segmentListWriteLock.unlock(); throw exception
    case Success(_) => segmentListWriteLock.unlock()
  }


  def put(key: String, value: String): Unit = {
    segmentListReadLock.lock()
    Try(availableSegment().add(key, value)) match {
      case Failure(exception) => segmentListReadLock.unlock(); throw exception
      case Success(_) => segmentListReadLock.unlock()
    }
  }


  def get(key: String): Option[String] = {
    segmentListReadLock.lock()
    Try {
      segments.view.reverse.map(_.get(key)).find(_ != NoInfo()).getOrElse(NotInDatabase()) match {
        case NotInDatabase() => None
        case HasValue(value) => Option(value)
        case NoInfo() => throw new IllegalStateException("Should have been filtered")
      }
    }
    match {
      case Failure(exception) => segmentListReadLock.unlock(); throw exception
      case Success(res) => segmentListReadLock.unlock(); res
    }
  }

  def remove(key: String): Unit = {
    segmentListReadLock.lock()
    Try {
      availableSegment().setDeleteFlag(key)
    } match {
      case Failure(exception) => segmentListReadLock.unlock(); throw exception
      case Success(_) => segmentListReadLock.unlock()
    }
  }

  def availableSegment(): Segment = {
    if (segments.isEmpty || segments.last.size > fileLimit) {
      segmentListReadLock.unlock()
      segmentListWriteLock.lock()
      Try {
        //double check as between lock the state might have changed
        if (segments.isEmpty || segments.last.size > fileLimit) {
          val id = fileId.incrementAndGet()
          segments += new Segment(id, logFolder)
          scheduler.notifySegmentAdded(this)
        }
      } match {
        //in case of success reacquire read lock, as it has been hold
        case Success(_) => segmentListWriteLock.unlock(); segmentListReadLock.lock()
        //just realease all locks
        case Failure(exception) => segmentListWriteLock.unlock(); throw exception
      }
    }
    segments.last
  }
}
