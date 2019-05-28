package org.tzotopia.affiliate

import java.net.URL
import java.util.concurrent.Executors

import cats.data.Kleisli
import cats.effect.{ContextShift, ExitCode, IO, IOApp}
import cats.implicits._
import org.http4s.{Header, HttpRoutes, Request, Response, StaticFile}
import org.http4s.implicits._
import org.http4s.dsl.io._
import org.http4s.server.blaze.BlazeServerBuilder

import scala.concurrent.ExecutionContext
import scala.concurrent.duration._

final class AffiliateRoutes(P: Products, C: AppConfig) {
  object UniqueColumnQueryParamMatcher extends QueryParamDecoderMatcher[String]("uniqueColumn")
  object JoinOnQueryParamMatcher extends QueryParamDecoderMatcher[String]("joinOn")

  private val blockingEc = ExecutionContext.fromExecutorService(Executors.newFixedThreadPool(4))
  private implicit val cs: ContextShift[IO] = IO.contextShift(ExecutionContext.global)

  //http://localhost:8080/awin?uniqueColumn=aw_product_id&joinOn=%7C
  def routes: Kleisli[IO, Request[IO], Response[IO]] = HttpRoutes.of[IO] {
    case request @ GET -> Root / "awin" :? UniqueColumnQueryParamMatcher(uniqueColumn) +& JoinOnQueryParamMatcher(joinOn) =>
      for {
        conf     <- C.affiliateConfig("awin")
        csv      <- P.processAffiliateResource(new URL(conf.right.get.url), uniqueColumn, joinOn.toCharArray.head)
        response <- StaticFile.fromFile(csv, blockingEc, Some(request)).getOrElseF(NotFound())
      } yield response.withHeaders(
        Header("Content-Type", "text/csv"),
        Header("Content-Disposition", "attachment; filename=\"awin.csv\"")
      )
  }.orNotFound
}

object Affiliate extends IOApp {
  val productsService = new CsvProducts
  val config = new PureConfig

  override def run(args: List[String]): IO[ExitCode] = {
    BlazeServerBuilder[IO]
      .withIdleTimeout(1 minute)
      .withResponseHeaderTimeout(1 minute)
      .bindHttp(8080, "localhost")
      .withHttpApp(new AffiliateRoutes(productsService, config).routes)
      .resource
      .use(_ => IO.never)
      .as(ExitCode.Success)
  }
}
