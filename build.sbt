import Dependencies._

lazy val root = (project in file("."))

organization := "de.wittig.gunther"
scalaVersion := "2.13.1"
version := "0.1.0-SNAPSHOT"
name := "akka-streams-cheatsheet"

libraryDependencies ++= Seq(
  akkaStream,
  alpakka,
  ammonite,
  scalaTest % Test
)

sourceGenerators in Test += Def.task {
  val file = (sourceManaged in Test).value / "amm.scala"
  IO.write(file, """object amm extends App { ammonite.Main.main(args) }""")
  Seq(file)
}.taskValue
