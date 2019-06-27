name := "affiliate-aggregator"

version := "0.1"

scalaVersion := "2.12.8"

scalacOptions ++= Seq(
  "-feature",
  "-deprecation",
  "-unchecked",
  "-language:postfixOps",
  "-language:higherKinds",
  "-Ypartial-unification")

val http4sVersion = "0.20.1"
val scalaTestVersion = "3.0.5"
val fs2Version = "1.0.4"
val zioVersion = "1.0.0-RC8-4"
val betterFilesVersion = "3.8.0"

libraryDependencies ++= Seq(
  "org.http4s" %% "http4s-dsl" % http4sVersion,
  "org.http4s" %% "http4s-blaze-server" % http4sVersion,
  "org.http4s" %% "http4s-circe" % http4sVersion,
  "co.fs2" %% "fs2-core" % fs2Version,

  "dev.zio" %% "zio" % zioVersion,
  "dev.zio" %% "zio-interop-cats" % zioVersion,

  "co.fs2" %% "fs2-core" % fs2Version,
  "co.fs2" %% "fs2-io" % fs2Version,

  "com.github.pureconfig"%% "pureconfig" % "0.11.0",

  "com.github.pathikrit" %% "better-files" % betterFilesVersion,

  "org.scalactic" %% "scalactic" % scalaTestVersion,
  "org.scalatest" %% "scalatest" % scalaTestVersion % "test",
  "org.scalacheck" %% "scalacheck" % "1.14.0" % "test"
)