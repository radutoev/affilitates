package org.tzotopia.affiliate

import java.io.File
import java.net.URL
import java.util.UUID
import java.util.concurrent.{Executors, TimeUnit}

import cats.effect.{Clock, ContextShift, IO}
import cats.implicits._
import fs2.{io, text}

import scala.concurrent.ExecutionContext
import scala.concurrent.duration.TimeUnit

trait Products {
  def processAffiliateResource(affiliateName: String,
                               url: URL,
                               uniqueColumn: String,
                               columnsToJoin: Vector[String],
                               joinOn: Char): IO[File]
}

final class CsvProducts(C: AppConfig) extends Products {
  private val blockingEc = ExecutionContext.fromExecutorService(Executors.newFixedThreadPool(4))

  implicit val cs: ContextShift[IO] = IO.contextShift(ExecutionContext.global)

  def processAffiliateResource(affiliateName: String,
                               url: URL,
                               uniqueColumn: String,
                               columnsToJoin: Vector[String],
                               joinOn: Char): IO[File] =
    for {
      dir       <- C.workdir
      outputDir <- C.outputDir
      name      <- IO("affilinet_products_1062_858833")
//      name      <- IO(UUID.randomUUID().toString)
//      gzip      = new File(dir,s"${name}.gz")
      orig      = new File(dir,s"${name}.csv")
//      _     <- Files.readFromUrl(url, gzip)
//      _     <- Files.unpack(gzip, orig)
      data      <- parseFile(orig, uniqueColumn, columnsToJoin, joinOn)
      dest      =  new File(outputDir,s"${name}.csv")
      count     <- Files.writeToCsv(data, dest)
//      _     <- IO(gzip.delete())
//      _     <- IO(orig.delete())
      _         <- IO(println("Cleanup complete"))
    } yield dest

  private[affiliate] def parseFile(file: File, uniqueColumn: String, columnsToJoin: Vector[String], joinOn: Char): IO[List[String]] =
    io.file.readAll[IO](file.toPath, blockingEc, 4096)
      .through(text.utf8Decode)
      .through(Fs2Csv.parse(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)"))
      .noneTerminate
      .compile
      .toList
      //out of the stream world.
      .map(groupProducts(_, uniqueColumn)(columnsToJoin)(joinOn))
      .map(transformToCsv)

  private[affiliate] def groupProducts(productRows: List[Option[Map[String, String]]], lookup: String)
                                      (columnsToGroup: Vector[String] = Vector.empty)
                                      (joinOn: Char = '|'): List[Map[String, String]] =
    productRows
      .filter(_.isDefined)
      .map(_.get)
      .filter(_.contains(lookup)) //implicitly skip over items with no key.
      .groupBy(_ (lookup))
      .map {
        case(key, list) => (key, list.reduce((m1, m2) => m1.map {
          case(header, value) =>
            val secondValue = m2.getOrElse(header, "")

            if(columnsToGroup.nonEmpty && !columnsToGroup.contains(header.toLowerCase)) {
              (header, value)
            } else {
              if(secondValue.nonEmpty) {
                if(value.nonEmpty) {
                  if(secondValue != value) (header, s"$value$joinOn${applyColumnTransformations(secondValue, joinOn)}")
                  else (header, value)
                }
                else (header, secondValue)
              } else {
                (header, value)
              }
            }
        }))
      }.values.toList

  /**
    * If further transformations are required revisit the design of the tx steps so as not to accumulate too many
    * method params.
    */
  private[affiliate] def applyColumnTransformations(colValue: String, joinOn: Char): String =
    colValue replaceAll("[,]", joinOn.toString)


  private[affiliate] def transformToCsv(listOfProductsWithHeaders: List[Map[String, String]]): List[String] = {
    val keys = listOfProductsWithHeaders.head.keySet.toList

    keys.mkString(",") :: listOfProductsWithHeaders.map(rowWithHeader => {
      keys
        .map { key => rowWithHeader.getOrElse(key, "") }
        .map { column =>
          if(column.contains(",")) {
            column replaceAll("[,]", "")
          } else {
            column
          }
        }
        .mkString(",")
    })
  }
}
