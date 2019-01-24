package com.scalask.data

import java.io.{BufferedReader, FileReader}
import java.util.zip.CRC32

import com.scalask.model._
import com.typesafe.scalalogging.LazyLogging

sealed trait Entry {
  def getBytes: Array[Byte] = toString.getBytes()

  def length = toString.length
}

case class RemoveFlag(key: String) extends Entry {
  private lazy val entry = key.length + "," + "DEL" + "," + key + "," + Entry.checksum(key) + '\n'

  override def toString: String = entry
}

//TODO delete fields
case class KeyVal(key: String, value: String) extends Entry {
  private lazy val prefix = key.length + "," + value.length + ","
  private lazy val entry = prefix + key + value + "," + Entry.checksum(key + value) + '\n'
  private lazy val valueIndex_ = prefix.length + key.length

  def valueIndex: Int = valueIndex_

  override def toString: String = entry

}


object Entry extends LazyLogging {
  private val checksumLen = 4

  def checksum(str: String): String = {
    val crc = new CRC32 // cannot be shared
    crc.reset()
    crc.update(str.getBytes)
    alignChecksum((crc.getValue % scala.math.pow(10, checksumLen).toInt).toString)
  }

  //rightPadding
  def alignChecksum(str: String): String = (1 to (checksumLen - str.length)).foldLeft(str)((cur, _) => cur + "0")

  def verify(str: String, chsum: String): Boolean = chsum == checksum(str)


  /**
    * Returns the entry and the seek offset for the value
    **/
  def fromFile(address: String): Stream[(Entry, Int)] = aux(new BufferedReader(new FileReader(address)), 0).filter(_.isDefined).flatten

  def fromFile(segment: Segment): Stream[(Entry, Int)] = fromFile(segment.getFileLocation)

  //optimize
  private def aux(bufferedReader: BufferedReader, offset: Int): Stream[Option[(Entry, Int)]] = {
    import Stream._
    bufferedReader.readLine() match {
      case null => empty
      case line: String => {
        val fullLine = ensureReadFully(line, bufferedReader)
        cons(parse(fullLine).map {
          case k: KeyVal => (k, k.valueIndex + offset)
          case r: RemoveFlag => (r, -1)
        }, aux(bufferedReader, offset + fullLine.length))
      }
    }
  }

  private def ensureReadFully(entryString: String, bufferedReader: BufferedReader): String = {
    val expectedLength = getEntryLen(entryString)
    val stringBuilder = new StringBuilder(entryString)
    stringBuilder.append("\n")

    if (stringBuilder.length < expectedLength) {
      val shouldRead = expectedLength - stringBuilder.length
      val buffer = new Array[Char](shouldRead)
      val readLen = bufferedReader.read(buffer)

      if (readLen != shouldRead) throw new IllegalStateException("corrupted files")
      stringBuilder.append(buffer)
    }
    stringBuilder.toString
  }


  private def parse(str: String): Option[Entry] = {
    val keyLenStr = str.takeWhile(_ != ',')
    val keyLenInt = keyLenStr.foldLeft(0)((cur, c) => cur * 10 + (c - '0'))

    val keyValChecksum = str.dropWhile(_ != ',').drop(1).dropWhile(_ != ',').drop(1)
    val key = keyValChecksum.slice(0, keyLenInt)
    val valLenOrDel = str.dropWhile(_ != ',').drop(1).takeWhile(_ != ',')

    val keyValue = keyValChecksum.takeWhile(_ != ',')
    val keyValueChecksum = keyValChecksum.dropWhile(_ != ',').slice(1, 1 + checksumLen)

    if (!verify(keyValue, keyValueChecksum)) {
      logger.warn(s"corrupt entry $keyValue $keyValueChecksum")
      return None
    }

    val entry = if (valLenOrDel == "DEL") {
      RemoveFlag(key)
    } else {
      val valLen = valLenOrDel.foldLeft(0)((acc, e) => 10 * acc + (e - '0'))
      val value = keyValChecksum.slice(keyLenInt, keyLenInt + valLen)
      KeyVal(key, value)
    }
    Some(entry)
  }

  private def getEntryLen(str: String): Int = {
    val keyLenStr = str.takeWhile(_ != ',')
    val keyLenInt = keyLenStr.foldLeft(0)((cur, c) => cur * 10 + (c - '0'))
    val valLenOrDelStr = str.dropWhile(_ != ',').drop(1).takeWhile(_ != ',')
    val valLenInt = valLenOrDelStr.foldLeft(0)((acc, e) => 10 * acc + (e - '0'))

    keyLenInt + keyLenStr.length + valLenOrDelStr.length + checksumLen + {
      if (valLenOrDelStr == "DEL") 0 else valLenInt
    } + 4 //3 commas 1 new line
  }
}
