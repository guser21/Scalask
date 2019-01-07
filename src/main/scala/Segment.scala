import java.io.RandomAccessFile
import java.nio.file.{Files, Paths, StandardOpenOption}
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.locks.ReentrantReadWriteLock

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.util.{Failure, Success, Try}


class Segment(var id: Int, val logFolder: String) {
  private val index: mutable.Map[String, ValueLocation] = new ConcurrentHashMap[String, ValueLocation]().asScala

  private val offset: AtomicInteger = new AtomicInteger(0)

  private var fileLocation = s"$logFolder/$id.log"
  private var filePath = Paths.get(fileLocation)
  if (!Files.exists(filePath)) Files.createFile(filePath)
  private var file = new RandomAccessFile(filePath.toString, "rw")

  private val lock = new ReentrantReadWriteLock()
  private val writeLock = lock.writeLock()
  private val readerLock = lock.readLock()

  private val removedKeys = ConcurrentHashMap.newKeySet[String]().asScala

  //  private val removedList =
  def getFileLocation = fileLocation

  //lock needed for offset
  def add(key: String, value: String): Unit = {
    writeLock.lock()
    Try {
      val entry = KeyVal(key, value)
      Files.write(filePath, entry.getBytes, StandardOpenOption.APPEND)
      index += ((key, ValueLocation(entry.valueIndex + offset.get, value.length)))
      offset.getAndAdd(entry.length)

      removedKeys.remove(key)
    } match {
      case Failure(exception) => writeLock.unlock(); throw exception
      case Success(_) => writeLock.unlock()
    }
  }

  def get(key: String): KeyState = {
    readerLock.lock()
    Try {
      if (removedKeys.contains(key)) return NotInDatabase()
      index.get(key) match {
        case None => NoInfo()
        case Some(value) => HasValue(getValue(value))
      }
    } match {
      case Failure(exception) => readerLock.unlock(); throw exception
      case Success(value) => readerLock.unlock(); value
    }
  }


  private def getValue(loc: ValueLocation): String = {
    //lock is held form get
      val bytes = new Array[Byte](loc.length)
      file.seek(loc.offset)
      file.read(bytes)
      new String(bytes)
  }

  def setDeleteFlag(key: String): Unit = {
    writeLock.lock()

    Try {

      val entry = RemoveFlag(key)
      Files.write(filePath, entry.getBytes, StandardOpenOption.APPEND)
      offset.getAndAdd(entry.length)

      removedKeys.add(key)
    } match {
      case Failure(exception) => writeLock.unlock(); throw exception
      case Success(_) => writeLock.unlock()
    }
  }

  def removeFromIndex(key: String): Unit = {
    writeLock.lock()
    Try {
      index.remove(key)
    } match {
      case Failure(exception) => writeLock.unlock(); throw exception
      case Success(_) => writeLock.unlock()
    }

  }

  def size: Int = offset.get()

  def delete(): Unit = {
    writeLock.lock()
    Try {
      file.close()
      Files.delete(filePath)
    } match {
      case Failure(exception) => writeLock.unlock(); throw exception
      case Success(_) => writeLock.unlock()
    }
  }

  //
  def contains(key: String): Boolean = {
    readerLock.lock()
    Try {
      index.contains(key) && !removedKeys.contains(key)
    } match {
      case Failure(exception) => readerLock.unlock(); throw exception
      case Success(value) => readerLock.unlock(); value
    }
  }

  /**
    * Loads from the specified file then returns list of elements need to be removed from previous blocks
    **/
  //TODO move to static companion object
  def load(): List[String] = {
    Entry.fromFile(filePath.toString).flatMap { case (entry, valueOffset) =>
      entry match {
        case kv: KeyVal => index += ((kv.key, ValueLocation(valueOffset, kv.value.length))); None
        case r: RemoveFlag => index.remove(r.key); removedKeys.add(r.key); Option(r.key)
      }
    }.toList
  }

  def reassignId(newId: Int): Unit = {

    val newFileLocation = s"$logFolder/$newId.log"
    val newPath = Paths.get(newFileLocation)
    writeLock.lock()
    Try(Files.move(filePath, newPath, java.nio.file.StandardCopyOption.REPLACE_EXISTING)) match {
      case Success(_) => {
        id = newId
        fileLocation = newFileLocation
        filePath = newPath
        file.close()
        file = new RandomAccessFile(filePath.toString, "rw")
        writeLock.unlock()
      }
      case Failure(exception) => writeLock.unlock(); throw exception
    }
  }


}
