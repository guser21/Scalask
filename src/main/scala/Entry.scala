import java.io.{BufferedReader, FileReader}
import java.nio.charset.StandardCharsets
import java.util.zip.CRC32

//TODO one more abstraction of Entry
//TODO how to have all of this abstracted with RemoveFlag as well
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

object Entry {
  private val checkSumLen = 8

  private val crc32Cksum = new CRC32

  def checksum(str: String): String = {
    crc32Cksum.reset()
    crc32Cksum.update(str.getBytes)
    BinaryConverter.longToBinary(crc32Cksum.getValue)
  }

  def verify(str: String, chsum: String): Boolean = chsum == checksum(str)


  /**
    * Returns the entry and the seek offset for the value
    **/
  def fromFile(address: String): Stream[(Entry, Int)] = aux(new BufferedReader(new FileReader(address)), 0)

  def fromFile(segment: Segment): Stream[(Entry, Int)] = fromFile(segment.getFileLocation)

  private def aux(bufferedReader: BufferedReader, offset: Int): Stream[(Entry, Int)] = {
    import Stream._
    bufferedReader.readLine() match {
      case null => empty
      case line: String => {
        val fullLine = ensureReadFully(line, bufferedReader)
        cons(parse(fullLine) match {
          case k: KeyVal =>
            {
              5;
              (k, k.valueIndex + offset)

            }
          case r: RemoveFlag => (r, -1)
        }, aux(bufferedReader, offset + fullLine.getBytes(StandardCharsets.US_ASCII).length))
      }
    }
  }

  private def ensureReadFully(entryString: String, bufferedReader: BufferedReader): String = {

    val expectedLength = getEntryLen(entryString)
    var fullString = entryString + "\n"

    if (fullString.length < expectedLength) {
      val shouldRead = expectedLength - fullString.length
      val buffer = new Array[Char](shouldRead)
      val readLen = bufferedReader.read(buffer)

      if (readLen != shouldRead) throw new IllegalStateException("corrupted files")
      fullString += new String(buffer)
    }
    fullString
  }


  private def parse(str: String): Entry = {
    val keyLenStr = str.takeWhile(_ != ',')
    val keyLenInt = keyLenStr.foldLeft(0)((cur, c) => cur * 10 + (c - '0'))

    val keyValChecksum = str.dropWhile(_ != ',').drop(1).dropWhile(_ != ',').drop(1)
    val key = keyValChecksum.slice(0, keyLenInt)
    val valLenOrDel = str.dropWhile(_ != ',').drop(1).takeWhile(_ != ',')

    val keyVal = keyValChecksum.takeWhile(_ != ',')
    val checksum = keyValChecksum.dropWhile(_ != ',').slice(1, 9)

    if (!verify(keyVal, checksum)) throw new IllegalStateException(s"corrupt files $keyVal $checksum")

    if (valLenOrDel == "DEL") {
      RemoveFlag(key)
    } else {

      val valLen = valLenOrDel.foldLeft(0)((acc, e) => 10 * acc + (e - '0'))
      val value = keyValChecksum.slice(keyLenInt, keyLenInt + valLen)

      KeyVal(key, value)
    }
  }

  private def getEntryLen(str: String): Int = {
    val keyLenStr = str.takeWhile(_ != ',')
    val keyLenInt = keyLenStr.foldLeft(0)((cur, c) => cur * 10 + (c - '0'))
    val valLenOrDelStr = str.dropWhile(_ != ',').drop(1).takeWhile(_ != ',')
    val valLenInt = valLenOrDelStr.foldLeft(0)((acc, e) => 10 * acc + (e - '0'))

    keyLenInt + keyLenStr.length + valLenOrDelStr.length + checkSumLen + {
      if (valLenOrDelStr == "DEL") 0 else valLenInt
    } + 4 //3 commas 1 new line
  }
}
