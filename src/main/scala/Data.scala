import java.io.RandomAccessFile

object Data {
  private val checkSumLength = 4

  def encode(key: String, value: String): String = {
    val entry = key + value
    val keyValue = key.length + "," + value.length + "," + entry + "," + checkSum(entry)
    keyValue.length + "," + keyValue
  }

  //TODO ask for better solution not to pass file as an argument
  def decode(file: RandomAccessFile): (String, String) = {
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

    val key = suffix.slice(0, keyLen )
    val value = suffix.slice(keyLen , keyLen + valLen)
    val checksum = suffix.substring(keyLen + valLen + 1)

    if (!checkWithCheckSum(key, value, checksum)) throw new RuntimeException()

    (key, value)
  }

  def checkWithCheckSum(key: String, value: String, checksum: String): Boolean = {
    true
  }

  def checkSum(data: String): String = {
    "0000"
  }
}
