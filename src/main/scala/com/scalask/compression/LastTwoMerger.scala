package com.scalask.compression

import com.scalask.data._
import com.scalask.model._
import com.typesafe.scalalogging.LazyLogging

import scala.collection.mutable
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

object LastTwoMerger extends Merger with LazyLogging {

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

    logger.debug(s"Time $millis: merging ${seg1.id} and ${seg2.id}")

    Entry.fromFile(seg2).map(_._1).reverse.foreach {
      //if in seg2 index =>  no del flag in this segment for that key
      case KeyVal(key, value) => if (seg2.contains(key) && !mergeSegment.contains(key)) mergeSegment.put(key, value)
      case RemoveFlag(key) => removedKeys.add(key)
    }
    Entry.fromFile(seg1).map(_._1).reverse.flatMap {
      case KeyVal(key, value) => Option(key, value)
      case RemoveFlag(key) => removedKeys.add(key); None
    }.filter { case (key, _) => !mergeSegment.contains(key) }
      .filter { case (key, _) => !removedKeys.contains(key) }
      .filter { case (key, _) => seg1.contains(key) }
      .foreach { case (key, value) => mergeSegment.put(key, value) }

    segmentList.acquireSegListWriteLock({
      logger.debug(s"Time $millis: removing ${seg1.id} and ${seg2.id}")

      segmentList.segments -= seg1
      segmentList.segments -= seg2
      seg1.delete()
      seg2.delete()

      mergeSegment.reassignId(seg2.id)
      logger.debug(s"Time $millis: adding  ${seg2.id} as merge of ${seg1.id} ${seg2.id}")

      segmentList.segments.prepend(mergeSegment)
    })
  }

  private def millis = System.currentTimeMillis() % 1000 * 1000

  private def choose(segList: SegmentList): (Segment, Segment) = segList.acquireSegListReadLock({
    if (segList.segments.size < 3) throw new IllegalArgumentException("too few segments in seglist to merge")
    val first = segList.segments.head
    val second = segList.segments.drop(1).head
    (first, second)
  })

}
