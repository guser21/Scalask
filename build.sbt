import sbt.util

name := "scalask"

version := "0.1"

scalaVersion := "2.12.8"
sbtVersion := "1.2.8"

libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.5"
libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.5" % "test"

logLevel := util.Level.Error
