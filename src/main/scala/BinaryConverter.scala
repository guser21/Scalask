

object BinaryConverter {

  def longToBinary(num: Long): String = {
    val binaryLong = new Array[Char](8)

    binaryLong(0) = ((num >>> 56).toInt & 0xFF).toChar
    binaryLong(1) = ((num >>> 48).toInt & 0xFF).toChar
    binaryLong(2) = ((num >>> 40).toInt & 0xFF).toChar
    binaryLong(3) = ((num >>> 32).toInt & 0xFF).toChar
    binaryLong(4) = ((num >>> 24).toInt & 0xFF).toChar
    binaryLong(5) = ((num >>> 16).toInt & 0xFF).toChar
    binaryLong(6) = ((num >>> 8).toInt & 0xFF).toChar
    binaryLong(7) = ((num >>> 0).toInt & 0xFF).toChar

    new String(binaryLong)
  }

  def binaryToLong(str: String): Long = {
    (binaryToInt(str.substring(0, 4)) << 32).toLong + (binaryToInt(str.substring(4)) & 0xFFFFFFFFL).toLong
  }

  private def binaryToInt(str: String): Int = {
    if (str.length != 4) throw new IllegalArgumentException

    val ch1: Int = str(0)
    val ch2: Int = str(1)
    val ch3: Int = str(2)
    val ch4: Int = str(3)
    if ((ch1 | ch2 | ch3 | ch4) < 0) throw new IllegalArgumentException
    (ch1 << 24) + (ch2 << 16) + (ch3 << 8) + (ch4 << 0)
  }
}
