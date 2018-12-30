import java.io.{File, FileWriter, RandomAccessFile}
import java.nio.file.{Files, Paths, StandardOpenOption}
import java.util.concurrent.atomic.AtomicInteger

import scala.collection.mutable
//Todo concurrent map
case class Segment(fileName: String, index: mutable.HashMap[String, Int], offset: AtomicInteger = new AtomicInteger(0))

class Bitcask extends DB {
  private val fileId = new AtomicInteger(0)
  private val maxFileSize = 1024 * 1024 * 3 //bytes
  private val folder = "./log-files"
  private val segList = new mutable.ArrayBuffer[Segment]

  /// constructor
  init
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


  //TODO new file every x entries
  //TODO on start read all files
  override def put(key: String, value: String): Unit = {
    val filePath = if (fileId.get() == 0) {
      val id = fileId.incrementAndGet()
      val name = s"$id.log"
      val fullPath = folder + "/" + name
      segList.append(Segment(fullPath, new mutable.HashMap[String, Int]()))

      Files.createFile(Paths.get(fullPath))
    } else {
      val fullPath = s"$folder/${fileId.get()}.log"
      Paths.get(fullPath)
    }
    val entry = Data.encode(key, value) + "\n"
    val offset = segList.last.offset
    Files.write(filePath, entry.getBytes(), StandardOpenOption.APPEND)

    segList.last.index.+=((key, offset.get()))
    offset.addAndGet(entry.length)

  }

  override def get(key: String): Option[String] = {
    val keySegment = segList
      .reverse
      .find(seg => seg.index.contains(key)) match {
      case None => return None
      case Some(value) => value
    }

    //something might go wrong
    //change to concurrent HashMap
    keySegment.index.get(key) match {
      case None => None
      case Some(value) => Some(getValue(keySegment.fileName, value))
    }
  }

  private def getValue(fileName: String, offset: Int): String = {
    val file = new RandomAccessFile(fileName, "r")
    file.seek(offset)
    val (key, value) = Data.decode(file)
    value
  }


  override def remove(key: String): Unit = {

  }

}
