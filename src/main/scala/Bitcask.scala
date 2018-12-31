class Bitcask extends DB {
  private val folder = "./log-files/"
  val segList = new SegList(folder)

  //TODO on start read all files
  override def put(key: String, value: String): Unit = segList.put(key, value)

  override def get(key: String): Option[String] = segList.get(key)

  override def remove(key: String): Unit = segList.remove(key)

}
