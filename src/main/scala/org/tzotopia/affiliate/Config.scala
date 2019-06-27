package org.tzotopia.affiliate

import java.io.File

import pureconfig.ConfigReader
import pureconfig.generic.auto._
import zio.{IO, Task, UIO, ZIO}

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

  def affiliateConfig(name: String): IO[RuntimeException, AffiliateConfig]
  def workdir: UIO[File]
  def outputDir: UIO[File]
}

final class PureConfig extends AppConfig {
  private val config: IO[RuntimeException, Config] = ZIO.fromEither(pureconfig.loadConfig[Config] match {
    case Right(conf) => Right(conf)
    case Left(failures) => Left(new RuntimeException(failures.head.description))
  })

  override def affiliateConfig(name: String): IO[RuntimeException, AffiliateConfig] =
    config.map(c => c.affiliates.get(name) match {
      case Some(affiliateConfig) => affiliateConfig
      case None => throw new RuntimeException
    })

  override def workdir: UIO[File] = config.map(_.workdir).orDie

  override def outputDir: UIO[File] = config.map(_.outputDir).orDie
}
