package com.scalask.model

import java.io.RandomAccessFile
import java.nio.file.{Files, Paths, StandardOpenOption}
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.locks.{ReadWriteLock, StampedLock}

import com.scalask.data._
import com.typesafe.scalalogging.LazyLogging

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.util.{Failure, Success, Try}

class Segment(var id: Int, val logFolder: String) extends LazyLogging {

  private val index: mutable.Map[String, ValueLocation] = new ConcurrentHashMap[String, ValueLocation]().asScala
  private val removedKeys = ConcurrentHashMap.newKeySet[String]().asScala

  private val offset: AtomicInteger = new AtomicInteger(0)
  private val lock: ReadWriteLock = new StampedLock().asReadWriteLock()
  private val writeLock = lock.writeLock()
  private val readerLock = lock.readLock()

  private var fileLocation = s"$logFolder/$id.log"
  private var filePath = Paths.get(fileLocation)
  private var file = new RandomAccessFile(filePath.toString, "rw")

  if (!Files.exists(filePath)) Files.createFile(filePath)

  def getFileLocation = fileLocation

  //lock needed for offset
  def put(key: String, value: String): Unit = acquireWriteLock({
    logger.debug(s"segment-id: $id put - key: $key")

    val entry = KeyVal(key, value)
    Files.write(filePath, entry.getBytes, StandardOpenOption.APPEND)
    index += ((key, ValueLocation(entry.valueIndex + offset.get, value.length)))
    offset.getAndAdd(entry.length)

    removedKeys.remove(key)
  })


  def get(key: String): KeyState = acquireReadLock({
    logger.debug(s"segment-id: $id get - key: $key")

    if (removedKeys.contains(key)) {
      NotInDatabase()
    } else {
      index.get(key) match {
        case None => NoInfo()
        case Some(value) => HasValue(getValue(value))
      }
    }
  })


  private def getValue(loc: ValueLocation): String = {
    val cur_read_file = new RandomAccessFile(filePath.toString, "rw")

    val bytes = new Array[Byte](loc.length)
    cur_read_file.seek(loc.offset)
    cur_read_file.read(bytes)

    cur_read_file.close()
    new String(bytes)
  }

  def setDeleteFlag(key: String): Unit = acquireWriteLock({
    logger.debug(s"segment-id: $id setDeleteFlag - key: $key")

    val entry = RemoveFlag(key)
    Files.write(filePath, entry.getBytes, StandardOpenOption.APPEND)
    offset.getAndAdd(entry.length)
    removedKeys.add(key)
  })


  def removeFromIndex(key: String): Unit = acquireWriteLock({
    logger.debug(s"segment-id: $id removeFromIndex - key: $key")

    index.remove(key)
  })

  def size: Int = offset.get()

  def delete(): Unit = acquireWriteLock({
    logger.debug(s"segment-id: $id delete segment")

    file.close()
    Files.delete(filePath)
  })

  def contains(key: String): Boolean = acquireReadLock({
    logger.debug(s"segment-id: $id contains - key: $key")

    index.contains(key) && !removedKeys.contains(key)
  })


  /**
    * Loads from the specified file then returns list of elements need to be removed from previous blocks
    **/
  def load(): Stream[String] = Entry.fromFile(filePath.toString).flatMap { case (entry, valueOffset) =>
    entry match {
      case kv: KeyVal => index += ((kv.key, ValueLocation(valueOffset, kv.value.length))); None
      case r: RemoveFlag => index.remove(r.key); removedKeys.add(r.key); Option(r.key)
    }
  }

  def reassignId(newId: Int): Unit = acquireWriteLock({
    logger.debug(s"segment-id: $id reassignId - newId $newId")

    val newFileLocation = s"$logFolder/$newId.log"
    val newPath = Paths.get(newFileLocation)
    Files.move(filePath, newPath, java.nio.file.StandardCopyOption.REPLACE_EXISTING)
    fileLocation = newFileLocation
    filePath = newPath
    file.close()
    file = new RandomAccessFile(filePath.toString, "rw")
  })

  def acquireReadLock[T](criticalSection: => T): T = {
    readerLock.lock()
    Try {
      criticalSection
    } match {
      case Failure(exception) => readerLock.unlock(); throw exception
      case Success(value) => readerLock.unlock(); value
    }
  }

  def acquireWriteLock[T](criticalSection: => T): T = {

    if (!writeLock.tryLock()) {
      writeLock.lock()
    }
    Try {
      criticalSection
    } match {
      case Failure(exception) => writeLock.unlock(); throw exception
      case Success(res) => writeLock.unlock(); res
    }

  }

}
