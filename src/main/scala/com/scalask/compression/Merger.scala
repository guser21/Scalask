package com.scalask.compression

import com.scalask.model.SegmentList

trait Merger {
  def compress(segList: SegmentList): Unit

  def compressAsync(segList: SegmentList): Unit
}
