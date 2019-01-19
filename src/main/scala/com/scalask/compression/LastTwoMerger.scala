package com.scalask.compression

import java.util.logging.Logger

import com.scalask.data._
import com.scalask.model._

import scala.collection.mutable
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.util.{Failure, Success, Try}

object LastTwoMerger extends Merger {
  private val log = Logger.getLogger(Logger.GLOBAL_LOGGER_NAME)

  override def compressAsync(segList: SegmentList): Unit = Future {
    LastTwoMerger.synchronized {
      compress(segList)
    }
  }(global)

  def compress(segmentList: SegmentList): Unit = synchronized {
    val pair = choose(segmentList)
    //make sure seg1 is the oldest
    val (seg1, seg2) = if (pair._1.id > pair._2.id) pair.swap else pair

    val mergeSegment = new Segment(-seg2.id, seg2.logFolder)

    val removedKeys = new mutable.HashSet[String]()

    log.info(s"Time $milis: merging ${seg1.id} and ${seg2.id}")

    Entry.fromFile(seg2).map(_._1).reverse.foreach {
      //if in seg2 index =>  no del flag in this segment for that key
      case KeyVal(key, value) => if (seg2.contains(key) && !mergeSegment.contains(key)) mergeSegment.add(key, value)
      case RemoveFlag(key) => removedKeys.add(key)
    }
    Entry.fromFile(seg1).map(_._1).reverse.flatMap {
      case KeyVal(key, value) => Option(key, value)
      case RemoveFlag(key) => removedKeys.add(key); None
    }.filter { case (key, _) => !mergeSegment.contains(key) }
      .filter { case (key, _) => !removedKeys.contains(key) }
      .filter { case (key, _) => seg1.contains(key) }
      .foreach { case (key, value) => mergeSegment.add(key, value) }

    //TODO locking with interface
    segmentList.segmentListWriteLock.lock()
    Try {
      log.info(s"Time $milis: removing ${seg1.id} and ${seg2.id}")

      segmentList.segments -= seg1
      segmentList.segments -= seg2
      seg1.delete()
      seg2.delete()

      mergeSegment.reassignId(seg2.id)
      log.info(s"Time $milis: adding  ${seg2.id} as merge of ${seg1.id} ${seg2.id}")

      segmentList.segments.prepend(mergeSegment)
    } match {
      case Success(_) => segmentList.segmentListWriteLock.unlock()
      case Failure(exception) => segmentList.segmentListWriteLock.unlock(); throw exception
    }
  }

  private def milis = System.currentTimeMillis() % 1000 * 1000

  private def choose(segList: SegmentList): (Segment, Segment) = {
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
}
