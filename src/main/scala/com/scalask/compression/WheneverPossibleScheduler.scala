package com.scalask.compression

import com.scalask.model.SegmentList

class WheneverPossibleScheduler(merger: Merger) extends MergeScheduler {
  override def mergeSegments(segList: SegmentList): Unit = {
    if (segList.segments.size > 3) merger.compressAsync(segList)
  }
}


