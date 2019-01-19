package com.scalask

trait DB {
  def put(key: String, value: String)

  def get(key: String): Option[String]

  def remove(key: String)
}
