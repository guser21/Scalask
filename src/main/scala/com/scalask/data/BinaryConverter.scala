package com.scalask.data

object BinaryConverter {

  //only last 4bytes are important
  def unsignedIntToBinary(num: Long): Array[Byte] = longToBinary(num).slice(4, 8)

  def longToBinary(num: Long): Array[Byte] = {
    val binaryLong = new Array[Byte](8)

    binaryLong(0) = ((num >>> 56).toInt & 0xFF).toByte
    binaryLong(1) = ((num >>> 48).toInt & 0xFF).toByte
    binaryLong(2) = ((num >>> 40).toInt & 0xFF).toByte
    binaryLong(3) = ((num >>> 32).toInt & 0xFF).toByte
    binaryLong(4) = ((num >>> 24).toInt & 0xFF).toByte
    binaryLong(5) = ((num >>> 16).toInt & 0xFF).toByte
    binaryLong(6) = ((num >>> 8).toInt & 0xFF).toByte
    binaryLong(7) = ((num >>> 0).toInt & 0xFF).toByte

    binaryLong
  }

  def binaryToLong(str: String): Long = {
    (binaryToInt(str.substring(0, 4)) << 32).toLong + (binaryToInt(str.substring(4)) & 0xFFFFFFFFL)
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
