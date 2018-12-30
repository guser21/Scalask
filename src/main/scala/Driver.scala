import java.nio.file.{Files, Paths}

import sys.process._
object Driver extends App {
  "rm -rf ./log-files" !
  val bitcask: DB = new Bitcask
  bitcask.put("hey","barev")
  bitcask.put("joe","biden")
  bitcask.put("jim","biden")
  val hey=bitcask.get("hey")
  val joe=bitcask.get("joe")
  val wrong=bitcask.get("mck")
}
