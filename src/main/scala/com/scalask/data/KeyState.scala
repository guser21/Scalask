package com.scalask.data

sealed trait KeyState

case class HasValue(value: String) extends KeyState

case class NotInDatabase() extends KeyState

case class NoInfo() extends KeyState