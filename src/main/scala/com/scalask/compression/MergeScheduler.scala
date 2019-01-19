package com.scalask.compression

import com.scalask.model.SegmentList

trait MergeScheduler {
  def mergeSegments(segList: SegmentList)
}
