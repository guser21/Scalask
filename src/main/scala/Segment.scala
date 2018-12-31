import java.io.RandomAccessFile
import java.nio.file.{Files, Paths, StandardOpenOption}
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger

import scala.collection.JavaConverters._
import scala.collection.mutable

case class ValLocation(offset: Int, length: Int)

class Segment(val id: Int, val fileLocation: String) {
  private val index: mutable.Map[String, ValLocation] = new ConcurrentHashMap[String, ValLocation]().asScala
  private val offset: AtomicInteger = new AtomicInteger(0)

  //TODO if exists read from file
  private val filePath = Paths.get(fileLocation)
  if (!Files.exists(filePath)) Files.createFile(filePath)
  private val file = new RandomAccessFile(filePath.toString, "rw")


  //lock needed for offset
  def add(key: String, value: String): Unit = {
    val entry = KeyVal(key, value)
    Files.write(filePath, entry.getBytes, StandardOpenOption.APPEND)
    index += ((key, ValLocation(entry.valueIndex + offset.get, value.length)))
    offset.getAndAdd(entry.length)
  }

  def get(key: String): Option[String] = index.get(key) match {
    case None => None
    case Some(value) => Some(getValue(value))
  }

  //should be a lock
  def setDeleteFlag(key: String): Unit = {
    val entry = RemoveFlag(key)
    Files.write(filePath, entry.getBytes, StandardOpenOption.APPEND)
    offset.getAndAdd(entry.length)
  }

  def remove(key: String): Unit = index.remove(key)

  def size: Int = offset.get()

  def delete(): Unit = file.close()


  /**
    * Loads from the specified file then returns list of elements need to be removed from previous blocks
    **/
  def load(): List[String] = {
    Entry.fromFile(filePath.toString).flatMap { case (entry, valueOffset) =>
      entry match {
        case kv: KeyVal => index += ((kv.key, ValLocation(valueOffset, kv.value.length))); None
        case r: RemoveFlag => index.remove(r.key); Option(r.key)
      }
    }.toList
  }

  private def getValue(loc: ValLocation): String = {
    val bytes = new Array[Byte](loc.length)
    file.seek(loc.offset)
    file.read(bytes)
    new String(bytes)
  }

}
