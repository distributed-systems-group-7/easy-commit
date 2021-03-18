import sbt.Package._
import sbt._

scalaVersion := "2.12.8"

libraryDependencies ++= Vector (
  Library.vertx_lang_scala,
  Library.vertx_web,
  Library.vertx_web_client,
  Library.scalaTest       % "test",
  // Uncomment for clustering
  // Library.vertx_hazelcast,

  //required to get rid of some warnings emitted by the scala-compile
  Library.vertx_codegen
)

libraryDependencies += "com.fasterxml.jackson.module" %% "jackson-module-scala" % "2.9.0"

