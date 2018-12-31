import java.io.RandomAccessFile

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

sealed trait Entry {
  //TODO
  def getBytes: Array[Byte] = toString.getBytes()

  def length = toString.length


}

object Entry {
  def checkSum(str: String): String = "0000"

  private val checkSumLen = 4

  def parse(str: String): Entry = {
    //TODO checkSum verification
    val keyLenStr = str.takeWhile(_ != ',')
    val keyLenInt = keyLenStr.foldLeft(0)((cur, c) => cur * 10 + (c - '0'))

    val keyValChsum = str.dropWhile(_ != ',').drop(1).dropWhile(_ != ',').drop(1)
    val key = keyValChsum.slice(0, keyLenInt)

    //TODO bad code :(
    val valLenOrDel = str.dropWhile(_ != ',').drop(1).takeWhile(_ != ',')

    if (valLenOrDel == "DEL") {
      RemoveFlag(key)
    } else {

      val valLen = valLenOrDel.foldLeft(0)((acc, e) => 10 * acc + (e - '0'))
      val value = keyValChsum.slice(keyLenInt, keyLenInt + valLen)

      KeyVal(key, value)
    }
  }

   def getEntryLen(str: String): Int = {

    val keyLenStr = str.takeWhile(_ != ',')
    val keyLenInt = keyLenStr.foldLeft(0)((cur, c) => cur * 10 + (c - '0'))
    val valLenOrDelStr = str.dropWhile(_ != ',').drop(1).takeWhile(_ != ',')
    val valLenInt = valLenOrDelStr.foldLeft(0)((acc, e) => 10 * acc + (e - '0'))

    keyLenInt + keyLenStr.length + valLenOrDelStr.length + checkSumLen + {
      if (valLenOrDelStr == "DEL") 0 else valLenInt
    } + 4 //3 commas 1 new line

  }

  /**
    * Returns the entry and the seek offset for the value
    * TODO how to make a lazy stream out of this
    **/
  def fromFile(address: String): ListBuffer[(Entry, Int)] = {
    val res = new mutable.ListBuffer[(Entry, Int)]
    val file = new RandomAccessFile(address, "r")
    var offset = 0
    while (offset < file.length()) {
      var entryString = file.readLine() //the length of key and value should be on the same line
      entryString+="\n"//TODO proper encoding
      val entryLen = getEntryLen(entryString)

      if (entryString.length < entryLen) {
        val shouldRead = entryLen - entryString.length
        val buffer = new Array[Byte](shouldRead)
        val readLen = file.read(buffer)
        if (readLen != shouldRead) throw new IllegalStateException("corrupted files")
        entryString += new String(buffer)
      }

      val curEntry = parse(entryString)
      val valueIndex = curEntry match {
        case k: KeyVal => k.valueIndex + offset
        case _: RemoveFlag => -1
      }
      offset += entryString.length
      res.append((curEntry, valueIndex))
    }
    file.close()
    res
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
