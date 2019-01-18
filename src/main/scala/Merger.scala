trait Merger {
  def compress(segList: SegList) : Unit
  def compressAsync(segList: SegList) : Unit
}
