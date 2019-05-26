package org.tzotopia.affiliate

final case class AffiliateConfig(url: String) extends AnyVal

case class Config (
  affiliates: Map[String, AffiliateConfig]
)
