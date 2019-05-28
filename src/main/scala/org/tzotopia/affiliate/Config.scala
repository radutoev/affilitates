package org.tzotopia.affiliate

import java.io.File

import pureconfig.ConfigReader

final case class AffiliateConfig(url: String) extends AnyVal

object Config {
  implicit val affiliateReader: ConfigReader[AffiliateConfig] = ConfigReader.fromCursor[AffiliateConfig] { cur =>
    for {
      objCur <- cur.asObjectCursor
      urlCur <- objCur.atKey("url")
      url    <- urlCur.asString
    } yield AffiliateConfig(url)
  }
}

case class Config (
  affiliates: Map[String, AffiliateConfig],
  workdir: File
)
