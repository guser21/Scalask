package com.scalask

import com.scalask.compression.{LastTwoMerger, WheneverPossibleScheduler}
import com.scalask.model.SegmentList


class Scalask extends DB {

  val scheduler = new WheneverPossibleScheduler(LastTwoMerger)
  val segList = new SegmentList(folder, scheduler, 1024)
  private val folder = "./log-files/"

  override def put(key: String, value: String): Unit = segList.put(key, value)

  override def get(key: String): Option[String] = segList.get(key)

  override def remove(key: String): Unit = segList.remove(key)

}
