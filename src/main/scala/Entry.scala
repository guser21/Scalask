import java.io.{BufferedReader, FileReader}

//TODO one more abstraction of Entry
//TODO how to have all of this abstracted with RemoveFlag as well
sealed trait Entry {
  def getBytes: Array[Byte] = toString.getBytes()

  def length = toString.length
}


object Entry {
  private val checkSumLen = 4

  def checkSum(str: String): String = "0000"


  /**
    * Returns the entry and the seek offset for the value
    **/
  def fromFile(address: String): Stream[(Entry, Int)] = aux(new BufferedReader(new FileReader(address)), 0)

  private def aux(bufferedReader: BufferedReader, offset: Int): Stream[(Entry, Int)] = {
    import Stream._
    bufferedReader.readLine() match {
      case null => empty
      case line: String => cons(
        parse(ensureReadFully(line, bufferedReader)) match {
          case k: KeyVal => (k, k.valueIndex + offset)
          case r: RemoveFlag => (r, -1)
        }, aux(bufferedReader, offset + line.length + 1))
    }
  }

  def fromFile(segment: Segment): Stream[(Entry, Int)] = fromFile(segment.getFileLocation)

  private def ensureReadFully(entryString: String, bufferedReader: BufferedReader): String = {

    val entryLen = getEntryLen(entryString)
    var fullString = entryString
    if (entryString.length < entryLen) {
      // the -1 is the new line character
      val shouldRead = entryLen - entryString.length - 1
      val buffer = new Array[Char](shouldRead)
      val readLen = bufferedReader.read(buffer)

      if (readLen != shouldRead) throw new IllegalStateException("corrupted files")
      fullString += new String(buffer)
    }
    fullString
  }


  private def parse(str: String): Entry = {
    //TODO checkSum verification
    val keyLenStr = str.takeWhile(_ != ',')
    val keyLenInt = keyLenStr.foldLeft(0)((cur, c) => cur * 10 + (c - '0'))

    val keyValChsum = str.dropWhile(_ != ',').drop(1).dropWhile(_ != ',').drop(1)
    val key = keyValChsum.slice(0, keyLenInt)
    val valLenOrDel = str.dropWhile(_ != ',').drop(1).takeWhile(_ != ',')

    if (valLenOrDel == "DEL") {
      RemoveFlag(key)
    } else {

      val valLen = valLenOrDel.foldLeft(0)((acc, e) => 10 * acc + (e - '0'))
      val value = keyValChsum.slice(keyLenInt, keyLenInt + valLen)

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


case class RemoveFlag(key: String) extends Entry {
  private lazy val entry = key.length + "," + "DEL" + "," + key + "," + Entry.checkSum(key) + '\n'

  override def toString: String = entry
}

case class KeyVal(key: String, value: String) extends Entry {
  private lazy val prefix = key.length + "," + value.length + ","
  private lazy val entry = prefix + key + value + "," + Entry.checkSum(key + value) + '\n'
  private lazy val valueIndex_ = prefix.length + key.length

  def valueIndex: Int = valueIndex_

  override def toString: String = entry

}
