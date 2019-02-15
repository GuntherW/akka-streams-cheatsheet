import sbt._

object Dependencies {

  val akkaVersion      = "2.5.21"
  val scalaTestVersion = "3.0.5"

  lazy val akkaStream = "com.typesafe.akka"  %% "akka-stream"              % akkaVersion
  lazy val alpakka    = "com.lightbend.akka" %% "akka-stream-alpakka-file" % "0.20"

  lazy val ammonite = "com.lihaoyi" % "ammonite" % "1.6.3" % "test" cross CrossVersion.full

  lazy val scalaTest = "org.scalatest" %% "scalatest" % scalaTestVersion
}
