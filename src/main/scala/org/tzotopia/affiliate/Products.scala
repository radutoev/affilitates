package org.tzotopia.affiliate

import java.io.File
import java.net.URL
import java.util.UUID
import java.util.concurrent.Executors

import cats.effect.{ContextShift, IO}
import cats.implicits._
import fs2.{io, text}
import org.tzotopia.affiliate.Fs2Csv.Columns

import scala.concurrent.ExecutionContext

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
      name      <- IO(UUID.randomUUID().toString)
      gzip      = new File(dir,s"${name}.gz")
      orig      = new File(dir,s"${name}.csv")
      _         <- Files.readFromUrl(url, gzip)
      _         <- Files.unpack(gzip, orig)
      data      <- parseFile(orig, uniqueColumn, columnsToJoin, joinOn)
      dest      =  new File(outputDir,s"${name}.csv")
      count     <- Files.writeToCsv(data, dest)
      _         <- cleanupWorkDir(gzip, orig)
    } yield dest

  private[affiliate] def parseFile(file: File,
                                   uniqueColumn: String,
                                   columnsToJoin: Vector[String],
                                   joinOn: Char): IO[List[String]] =
    io.file.readAll[IO](file.toPath, blockingEc, 4096)
      .through(text.utf8Decode)
      .through(Fs2Csv.parse(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)")(Option.empty[Columns]))
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
        case(key, list) =>
          (key, list.fold(Map.empty)((m1, m2) => m2 map {
            case (header, value) =>
              val prevValues = m1.getOrElse(header, "")
              if(columnsToGroup.nonEmpty && !columnsToGroup.contains(header.toLowerCase)) {
                (header, if(prevValues.nonEmpty) prevValues else value)
              } else {
                val transformed = applyValueTransformations(value, joinOn.toString)
                (header, if(prevValues.isEmpty) transformed else prevValues + joinOn.toString + transformed)
              }
          }))
      }
      .mapValues { _ map {
        case (key, joined) => (key, uniqueValues(joined, joinOn.toString))
      }}.values.toList


  private[affiliate] val makeNrColumn: String => String = colValue =>
    colValue replaceAll ("\\d[/]\\d[M]", "") replaceAll("[ ]", "") replaceAll("\\b(\\d{1,3})([a-zA-Z])\\b", "$1")

  private [affiliate] val uniqueValues: (String, String) => String = (colValue, sep) =>
    colValue.split(s"[$sep]").toSet.mkString(sep)

  private[affiliate] val applyValueTransformations: (String, String) => String = (colValue, joinOn) => {
    makeNrColumn (
      colValue replaceAll ("[,]", joinOn)
    ) replaceAll("[/]", joinOn)
  }

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

  private def cleanupWorkDir(gzip: File, csv: File): IO[Unit] = IO {
    gzip.delete()
    csv.delete()
    println("Cleanup complete")
  }
}
