package org.tzotopia.affiliate

import java.io.File

import cats.effect.IO

import pureconfig.ConfigReader
import pureconfig.generic.auto._

final case class AffiliateConfig(url: String) extends AnyVal

final case class Config (
  affiliates: Map[String, AffiliateConfig],
  workdir: File,
  outputDir: File
)

trait AppConfig {
  implicit val affiliateReader: ConfigReader[AffiliateConfig] = ConfigReader.fromCursor[AffiliateConfig] { cur =>
    for {
      objCur <- cur.asObjectCursor
      urlCur <- objCur.atKey("url")
      url    <- urlCur.asString
    } yield AffiliateConfig(url)
  }

  def affiliateConfig(name: String): IO[Either[Throwable, AffiliateConfig]]
  def workdir: IO[File]
  def outputDir: IO[File]
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
}
