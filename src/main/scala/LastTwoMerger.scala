import scala.collection.mutable
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.util.{Failure, Success, Try}

object LastTwoMerger extends Merger {
  def compress(segList: SegList): Unit = synchronized {
    val pair = choose(segList)
    //make sure seg1 is the oldest
    val (seg1, seg2) = if (pair._1.id > pair._2.id) pair.swap else pair

    val mergeSegment = new Segment(-seg2.id, seg2.logFolder)

    val removedKeys = new mutable.HashSet[String]()

    Entry.fromFile(seg2).map(_._1).foreach {
      //if in seg2 index =>  no del flag in this segment for that key
      case KeyVal(key, value) => if (seg2.contains(key)) mergeSegment.add(key, value)
      case RemoveFlag(key) => removedKeys.add(key)
    }
    Entry.fromFile(seg1).map(_._1).flatMap {
      case KeyVal(key, value) => if (seg1.contains(key)) Option(key, value) else None
      case RemoveFlag(_) => None
    }.filter { case (key, _) => !removedKeys.contains(key) }
      .foreach { case (key, value) => mergeSegment.add(key, value) }


    segList.segmentListWriteLock.lock()
    Try {
      segList.segments -= seg1
      segList.segments -= seg2
      seg1.delete()
      seg2.delete()

      mergeSegment.reassignId(seg2.id)
      segList.segments.prepend(mergeSegment)
    } match {
      case Success(_) => segList.segmentListWriteLock.unlock()
      case Failure(exception) => segList.segmentListWriteLock.unlock(); throw exception
    }
  }

  //TODO better choose
  // every 2
  private def choose(segList: SegList): (Segment, Segment) = {
    segList.segmentListReadLock.lock()
    Try {
      if (segList.segments.size < 3) throw new IllegalArgumentException("too few segments in seglist to merge")
      val first = segList.segments.head
      val second = segList.segments.drop(1).head
      (first, second)
    } match {
      case Success(res) => segList.segmentListReadLock.unlock(); res
      case Failure(exception) => segList.segmentListReadLock.unlock(); throw exception
    }
  }


  override def compressAsync(segList: SegList): Unit = Future {
    LastTwoMerger.synchronized{
      compress(segList)
    }
  }(global)
}
