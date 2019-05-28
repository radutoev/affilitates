package org.tzotopia.affiliate

import java.net.URL
import java.util.concurrent.Executors

import cats.effect.{ExitCode, IO, IOApp}
import cats.implicits._
import org.http4s.{Header, HttpRoutes, StaticFile}
import org.http4s.implicits._
import org.http4s.dsl.io._
import org.http4s.server.blaze.BlazeServerBuilder
import pureconfig.generic.auto._

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._

import Config.affiliateReader

object Affiliate extends IOApp {
  object UniqueColumnQueryParamMatcher extends QueryParamDecoderMatcher[String]("uniqueColumn")
  object JoinOnQueryParamMatcher extends QueryParamDecoderMatcher[String]("joinOn")

  private val blockingEc = ExecutionContext.fromExecutorService(Executors.newFixedThreadPool(4))
  private val config: Config = pureconfig.loadConfig[Config] match {
    case Right(conf) => conf
    case Left(failures) => {
      println(failures)
      throw new RuntimeException("Shit")
    }
  }

  //http://localhost:8080/awin?uniqueColumn=aw_product_id&joinOn=%7C
  private val affiliateRoutes = HttpRoutes.of[IO] {
    case request @ GET -> Root / "awin" :? UniqueColumnQueryParamMatcher(uniqueColumn) +& JoinOnQueryParamMatcher(joinOn) =>
      for {
        _        <- IO.unit
        csv      <- Products.processAffiliateResource(new URL(config.affiliates.getOrElse("awin", AffiliateConfig(url = "")).url), uniqueColumn, joinOn.toCharArray.head)
        response <- StaticFile.fromFile(csv, blockingEc, Some(request)).getOrElseF(NotFound())
      } yield response.withHeaders(
        Header("Content-Type", "text/csv"),
        Header("Content-Disposition", "attachment; filename=\"awin.csv\"")
      )
  }.orNotFound

  override def run(args: List[String]): IO[ExitCode] = {
    BlazeServerBuilder[IO]
      .withIdleTimeout(1 minute)
      .withResponseHeaderTimeout(1 minute)
      .bindHttp(8080, "localhost")
      .withHttpApp(affiliateRoutes)
      .resource
      .use(_ => IO.never)
      .as(ExitCode.Success)
  }
}
