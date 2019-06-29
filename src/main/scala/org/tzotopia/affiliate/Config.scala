package org.tzotopia.affiliate

import java.io.File

import cats.effect.IO
import pureconfig.error.ConfigReaderFailures
import pureconfig.{ConfigCursor, ConfigReader}
import pureconfig.generic.auto._

final case class Params(uniqueColumn: String, joinOn: String, columnsToJoin: String)

final case class AffiliateConfig(url: String, params: Params)

final case class Config (
  affiliates: Map[String, AffiliateConfig],
  workdir: File,
  outputDir: File
)

trait AppConfig {
  implicit val affiliateReader: ConfigReader[AffiliateConfig] = {
    val readStrValue: ConfigCursor => Either[ConfigReaderFailures, String] = cur => cur.asString

    ConfigReader.fromCursor[AffiliateConfig] { cur =>
      for {
        objCur <- cur.asObjectCursor
        url    <- objCur.atKey("url").flatMap(readStrValue)
        params <- objCur.atKey("params")
          .flatMap(_.asObjectCursor)
          .flatMap(c => for {
              uniqueCol     <- c.atKey("uniqueColumn").flatMap(readStrValue)
              joinOn        <- c.atKey("joinOn").flatMap(readStrValue)
              columnsToJoin <- c.atKey("columnsToJoin").flatMap(readStrValue)
            } yield Params(uniqueCol, joinOn, columnsToJoin)
          )
      } yield AffiliateConfig(url, params)
    }
  }

  def affiliateConfig(name: String): IO[Either[Throwable, AffiliateConfig]]
  def workdir: IO[File]
  def outputDir: IO[File]
  def affiliateNames: IO[Set[String]]
}

final class PureConfig extends AppConfig {
  private val config: IO[Config] = IO.fromEither(pureconfig.loadConfig[Config] match {
    case Right(conf) => Right(conf)
    case Left(failures) => Left(new RuntimeException(failures.head.description))
  })

  override def affiliateConfig(name: String): IO[Either[Throwable, AffiliateConfig]] =
    config.map(c => c.affiliates.get(name).toRight(new RuntimeException("config values not present")))

  override def workdir: IO[File] = config.map(_.workdir)

  override def outputDir: IO[File] = config.map(_.outputDir)

  override def affiliateNames: IO[Set[String]] = config.map(_.affiliates.keySet)
}
