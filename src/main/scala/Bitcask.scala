import java.io.File
import java.nio.file.{Files, Paths}
import java.util.concurrent.atomic.AtomicInteger


//Todo concurrent map

class Bitcask extends DB {
  private val fileId = new AtomicInteger(0)
  private val folder = "./log-files"
  /// constructor
  Files.createDirectory(Paths.get(folder))
  ///end constructor

  private def init = {
    if (Files.exists(Paths.get(folder))) {
      val log_folder = new File(folder)

      val last_id = log_folder.listFiles().toStream
        .map(f => (f.getName, f.getName.indexOf('.')))
        .map(p => p._1.substring(0, p._2))
        .map(Integer.parseInt)
        .max
      // TODO read files and parse hashmap
      fileId.set(last_id)
    } else {
      Files.createDirectory(Paths.get(folder))
    }

  }


  //TODO on start read all files
  override def put(key: String, value: String): Unit = SegList.put(key, value)

  override def get(key: String): Option[String] = SegList.get(key)

  override def remove(key: String): Unit = SegList.remove(key)

}
