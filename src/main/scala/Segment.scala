import java.io.RandomAccessFile
import java.nio.file.{Files, Paths, StandardOpenOption}
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger

import scala.collection.JavaConverters._
import scala.collection.mutable

class Segment(val location: String) {
  private val index: mutable.Map[String, Int] = new ConcurrentHashMap[String, Int]().asScala
  private val offset: AtomicInteger = new AtomicInteger(0)

  //TODO if exists read from file
  private val filePath = Paths.get(location) //TODO read from conf
  Files.createFile(filePath)
  private val file = new RandomAccessFile(filePath.toString, "rw")

  def add(key: String, value: String): Unit = {
    val keyVal = key + value
    val keyValue = key.length + "," + value.length + "," + keyVal + "," + checkSum(keyVal)
    val entry = keyValue.length + "," + keyValue + '\n'

    Files.write(filePath, entry.getBytes(), StandardOpenOption.APPEND)

    index += ((key, offset.get()))
    offset.getAndAdd(entry.length)
  }

  def get(key: String): Option[String] = {
    index.get(key) match {
      case None => None
      case Some(value) => Some(getKeyValue(value)._2)
    }
  }

  def size(): Int = offset.get()

  private def checkSum(entry: String): String = "0000"

  private def getKeyValue(offSet: Int): (String, String) = {
    file.seek(offSet)

    //we do it here as in encode the length could have increased
    val shouldRead = {
      var curChar = 0
      var res = 0
      while ( {
        curChar = file.read()
        curChar != ','
      }) {
        res *= 10
        res += curChar - '0'
      }
      res
    }

    val bytes = new Array[Byte](shouldRead)
    val readLength = file.read(bytes)

    if (readLength != shouldRead) throw new RuntimeException("Cannot decode")

    val entry = new String(bytes)

    val keyLen = entry
      .takeWhile(_ != ',')
      .map(_ - '0')
      .foldLeft(0) { (acc, e) => 10 * acc + e }

    val valLen = entry
      .dropWhile(_ != ',')
      .drop(1)
      .takeWhile(_ != ',')
      .map(_ - '0')
      .foldLeft(0) { (acc, e) => 10 * acc + e }

    val suffix = entry
      .dropWhile(_ != ',') //keyLen
      .drop(1)
      .dropWhile(_ != ',') //valLen
      .drop(1)

    val key = suffix.slice(0, keyLen)
    val value = suffix.slice(keyLen, keyLen + valLen)
    val checksum = suffix.substring(keyLen + valLen + 1)
    //TODO checksum
    //    if (!checkWithCheckSum(key, value, checksum)) throw new RuntimeException()

    (key, value)
  }

}
