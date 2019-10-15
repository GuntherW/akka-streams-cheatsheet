import sbt._

object Dependencies {

  val akkaVersion      = "2.5.25"
  val scalaTestVersion = "3.0.8"
  lazy val akkaStream = "com.typesafe.akka"  %% "akka-stream"              % akkaVersion
  lazy val alpakka    = "com.lightbend.akka" %% "akka-stream-alpakka-file" % "1.1.2"
  lazy val ammonite = "com.lihaoyi" % "ammonite" % "1.7.4" % "test" cross CrossVersion.full
  lazy val scalaTest = "org.scalatest" %% "scalatest" % scalaTestVersion
}
