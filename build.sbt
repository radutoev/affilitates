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
val catsVersion = "2.0.0-M1"

libraryDependencies ++= Seq(
  "org.http4s"           %% "http4s-dsl"            % http4sVersion,
  "org.http4s"           %% "http4s-blaze-server"   % http4sVersion,
  "org.typelevel"        %% "cats-core"             % catsVersion,
  "org.typelevel"        %% "cats-effect"           % catsVersion,
  "org.typelevel"        %% "cats-kernel"           % catsVersion,
  "org.typelevel"        %% "cats-macros"           % catsVersion,
  "co.fs2"               %% "fs2-core"              % fs2Version,
  "co.fs2"               %% "fs2-io"                % fs2Version,
  "eu.timepit"           %% "fs2-cron-core"         % "0.1.0",
  "com.github.pureconfig"%% "pureconfig"            % "0.11.0",
  "org.scalactic"        %% "scalactic"             % scalaTestVersion,
  "org.scalatest"        %% "scalatest"             % scalaTestVersion   % "test",
  "org.scalacheck"       %% "scalacheck"            % "1.14.0"           % "test"
)