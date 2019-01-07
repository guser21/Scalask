class Bitcask extends DB {
  private val folder = "./log-files/"
  val scheduler = new WheneverPossibleScheduler(LastTwoMerger)
  val segList = new SegList(folder, scheduler)

  override def put(key: String, value: String): Unit = segList.put(key, value)

  override def get(key: String): Option[String] = segList.get(key)

  override def remove(key: String): Unit = segList.remove(key)

}
