
class WheneverPossibleScheduler(merger: Merger) extends MergeScheduler {
  override def
  notifySegmentAdded(segList: SegList): Unit = {
    if (segList.segments.size > 3) merger.compressAsync(segList)
  }
}


