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
  object ColumnsToJoinQueryParamMatcher extends QueryParamDecoderMatcher[String](name = "columnsToJoin")

  private val blockingEc = ExecutionContext.fromExecutorService(Executors.newFixedThreadPool(4))
  private implicit val cs: ContextShift[IO] = IO.contextShift(ExecutionContext.global)

  //http://localhost:8080/<affiliate_name>?uniqueColumn=merchant_image_url&joinOn=%7C&columnsToJoin=custom_1
  def routes: Kleisli[IO, Request[IO], Response[IO]] = HttpRoutes.of[IO] {
    case request @ GET -> Root / affiliateName :? UniqueColumnQueryParamMatcher(uniqueColumn)
      +& JoinOnQueryParamMatcher(joinOn)
      +& ColumnsToJoinQueryParamMatcher(columnsToJoin) =>
      for {
        conf     <- C.affiliateConfig(affiliateName)
        csv      <- P.processAffiliateResource (
          affiliateName,
          new URL(conf.right.get.url),
          uniqueColumn,
          columnsToJoin.split(",").map(_.toLowerCase).toVector,
          joinOn.toCharArray.head
        )
        response <- StaticFile.fromFile(csv, blockingEc, Some(request)).getOrElseF(NotFound())
      } yield response.withHeaders(
        Header("Content-Type", "text/csv"),
        Header("Content-Disposition", "attachment; filename=\"" + affiliateName + ".csv\"")
      )
  }.orNotFound
}

object Affiliate extends IOApp {
  val config = new PureConfig
  val productsService = new CsvProducts(config)

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
