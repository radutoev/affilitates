package org.tzotopia.affiliate

import java.net.URL
import java.time.LocalTime
import java.util.concurrent.Executors

import cats.data.Kleisli
import cats.effect.{Clock, ContextShift, ExitCode, IO, IOApp}
import cats.implicits._
import cron4s.Cron
import eu.timepit.fs2cron.awakeEveryCron
import fs2._
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
  implicit val cs: ContextShift[IO] = IO.contextShift(ExecutionContext.global)
  implicit val clock: Clock[IO] = Clock.create[IO]

  def routes: Kleisli[IO, Request[IO], Response[IO]] = HttpRoutes.of[IO] {
    case request @ GET -> Root / affiliateName :? UniqueColumnQueryParamMatcher(uniqueColumn)
      +& JoinOnQueryParamMatcher(joinOn)
      +& ColumnsToJoinQueryParamMatcher(columnsToJoin) =>

      for {
        conf     <- C.affiliateConfig(affiliateName)
        maybeCsv <- P.getCsvForAffiliate(affiliateName)
        csv      <- maybeCsv.fold(P.processAffiliateResource (
          affiliateName,
          new URL(conf.right.get.url),
          uniqueColumn,
          columnsToJoin.split(",").map(_.toLowerCase).toVector,
          joinOn.toCharArray.head
        ))(IO(_))
        response <- StaticFile.fromFile(csv, blockingEc, Some(request)).getOrElseF(NotFound())
      } yield response.withHeaders(
        Header("Content-Type", "text/csv"),
        Header("Content-Disposition", "attachment; filename=\"" + affiliateName + ".csv\"")
      )


  }.orNotFound
}

object Affiliate extends IOApp {
  private val blockingEc = ExecutionContext.fromExecutorService(Executors.newCachedThreadPool)

  val config = new PureConfig
  val productsService = new CsvProducts(config)

  def cronInit: IO[Unit] = IO {
    //every 2 seconds
    val cron = Cron.unsafeParse("* */2 * ? * *")
    val printTime = Stream.eval(IO(println(LocalTime.now)))
    val generateAffiliateCSVs = Stream.eval(IO(config.affiliateNames.map(names => names.map(name => {
      for {
        conf <- config.affiliateConfig(name)
        _    <- IO(println(s"Processing for $name"))
        _    <- productsService.processAffiliateResource (
          name,
          new URL(conf.right.get.url),
          conf.right.get.params.uniqueColumn,
          conf.right.get.params.columnsToJoin.split(",").map(_.toLowerCase).toVector,
          conf.right.get.params.joinOn.toCharArray.head
        )
      } yield ()
    }))))

    val scheduled = awakeEveryCron[IO](cron) >> printTime >> generateAffiliateCSVs
    scheduled.compile.drain.unsafeRunSync()
  }

  override def run(args: List[String]): IO[ExitCode] = {
    val cron: IO[Unit] = contextShift.evalOn(blockingEc)(cronInit) //executes on blockingEc
    val server: IO[ExitCode] = BlazeServerBuilder[IO]
      .withIdleTimeout(2 minutes)
      .withResponseHeaderTimeout(2 minutes)
      .bindHttp(8080, "0.0.0.0")
      .withHttpApp(new AffiliateRoutes(productsService, config).routes)
      .resource
      .use(_ => IO.never)
      .as(ExitCode.Success)

    List(cron, server).parSequence.unsafeRunSync()

    server
  }
}
