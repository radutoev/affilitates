package org.tzotopia.affiliate

import java.io.File
import java.net.URL
import java.util.concurrent.Executors

import cats.effect.{ContextShift, IO}
import cats.implicits._
import fs2.{io, text}

import scala.concurrent.ExecutionContext

trait Products {
  def processAffiliateResource(url: URL, uniqueColumn: String, joinOn: Char): IO[File]
}

final class CsvProducts extends Products {
  private val blockingEc = ExecutionContext.fromExecutorService(Executors.newFixedThreadPool(4))

  implicit val cs: ContextShift[IO] = IO.contextShift(ExecutionContext.global)

  def processAffiliateResource(url: URL, uniqueColumn: String, joinOn: Char): IO[File] =
    for {
      _     <- IO.unit
      gzip  = new File("D:\\downloads\\csv-euri\\test.gz")
      orig = new File("D:\\downloads\\csv-euri\\test.csv")
      _     <- Files.readFromUrl(url, gzip)
      _     <- Files.unpack(gzip, orig)
      data  <- parseFile(orig, uniqueColumn, joinOn)
      dest  =  new File("D:\\downloads\\csv-euri\\test-generated.csv")
      count <- Files.writeToCsv(data, dest)
      _     <- IO(println(s"$count bytes copied from ${orig.getPath} to ${dest.getPath}"))
    } yield dest

  private[affiliate] def parseFile(file: File, uniqueColumn: String, joinOn: Char): IO[List[String]] =
    io.file.readAll[IO](file.toPath, blockingEc, 4096)
      .through(text.utf8Decode)
      .through(Fs2Csv.parse(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)"))
      .noneTerminate
      .compile
      .toList
      //out of the stream world.
      .map(groupProducts(_, uniqueColumn)(joinOn))
      .map(transformToCsv)

  private[affiliate] def groupProducts(productRows: List[Option[Map[String, String]]], lookup: String)(joinOn: Char = '|'): List[Map[String, String]] =
    productRows
      .filter(_.isDefined)
      .map(_.get)
      .filter(_.contains(lookup)) //implicitly skip over items with no key.
      .groupBy(_ (lookup))
      .map {
        case(key, list) => (key, list.reduce((m1, m2) => m1.map {
          case(header, value) =>
            val secondValue = m2.getOrElse(header, "")
            if(secondValue.nonEmpty) {
              if(value.nonEmpty) {
                if(secondValue != value) (header, s"$value$joinOn$secondValue")
                else (header, value)
              }
              else (header, secondValue)
            } else {
              (header, value)
            }
        }))
      }.values.toList

  private[affiliate] def transformToCsv(listOfProductsWithHeaders: List[Map[String, String]]): List[String] = {
    val keys = listOfProductsWithHeaders.head.keySet.toList

    keys.mkString(",") :: listOfProductsWithHeaders.map(rowWithHeader =>
       keys map { key => rowWithHeader.getOrElse(key, "") } mkString ","
    )
  }
}
