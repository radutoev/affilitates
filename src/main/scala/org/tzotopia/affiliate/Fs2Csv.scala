package org.tzotopia.affiliate

import fs2.{Pipe, Pipe2, RaiseThrowable, text}

object Fs2Csv {
  type ColumnNames = Vector[String]
  type ColumnValue

  def parse[F[_]: RaiseThrowable](sep: String)(columnsFilter: Option[ColumnNames] = None): Pipe[F, String, Map[String, String]] =
    parse(sep, columnsFilter, identity)

  def parseLenient[F[_], I](sep: String, columnsFilter: Option[ColumnNames]): Pipe[F, String, Either[Throwable, Map[String, String]]] =
    parseLenient(sep, columnsFilter, identity)

  def parse[F[_]: RaiseThrowable, I](sep: String, columnsFilter: Option[ColumnNames], convert: I => String): Pipe[F, I, Map[String, String]] =
    csvBytes => parseLenient(sep, columnsFilter, convert)(csvBytes).rethrow

  def parseLenient[F[_], I](sep: String, columnsFilter: Option[ColumnNames], convert: I => String): Pipe[F, I, Either[Throwable, Map[String, String]]] =
    csvBytes => {
      val lineStream: fs2.Stream[F, Vector[String]] = csvBytes
        .map(convert(_))
        .through(text.lines)
        .filter(_.trim.nonEmpty)
        .through(splitLine(sep))

      columnsFilter match {
        case None => lineStream.through(zipWithHeader)
        case Some(filterColumns) => lineStream.map((_, filterColumns)).through(zipWithHeaderAndFilterColumns)
      }
    }

  def splitLine[F[_]](sep: String): Pipe[F, String, Vector[String]] =
    _.map(_.split(sep, -1).toVector.map(_.trim))

  def zipWithHeader[F[_]]: Pipe[F, Vector[String], Either[Throwable, Map[String, String]]] =
    csvRows =>
      csvRows.zipWithIndex
        .mapAccumulate(Option.empty[Vector[String]]) {
          case (None, (headerRow, _)) =>
            (Some(headerRow), Right(Map.empty[String, String]))
          case (h @ Some(header), (row, rowIndex)) =>
            if (header.length == row.length)
              h -> Right(header.map(h => if(h.startsWith("\"") && h.endsWith("\"")) h.drop(1).dropRight(1) else h).zip(row).toMap)
            else
              h -> Left(HeaderSizeMismatch(rowIndex, header.length, row))
        }
        .drop(1)
        .map(_._2)

  def zipWithHeaderAndFilterColumns[F[_]]: Pipe[F, (Vector[String], ColumnNames), Either[Throwable, Map[String, String]]] =
    csvRows =>
      csvRows.zipWithIndex
        .mapAccumulate(Option.empty[Vector[String]]) {
          case (None, ((headerRow, columns), _)) =>
            (Some(headerRow), Right(Map.empty[String, String]))

          case (h @ Some(header), ((row, columns), rowIndex)) =>
            if (header.length == row.length)
              h -> Right(header.zip(row).toMap)
            else
              h -> Left(HeaderSizeMismatch(rowIndex, header.length, row))
        }
        .drop(1)
        .map(_._2)

  case class HeaderSizeMismatch(rowIndex: Long,
                                headerLength: Int,
                                row: Vector[String]) extends Throwable (
      s"CSV row at index $rowIndex has ${row.length} items, header has $headerLength. Row: $row"
  )
}
