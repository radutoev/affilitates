package org.tzotopia.affiliate

import java.io.{ByteArrayInputStream, File, FileInputStream, FileOutputStream, InputStream, OutputStream}
import java.net.URL
import java.util.zip.GZIPInputStream

import cats.effect.{IO, Resource}
import cats.implicits._

object Files {
  def readFromUrl(url: URL, destination: File): IO[Long] =
    (for {
      inStream  <- urlInputSteam(url)
      outStream <- outputStream(destination)
    } yield (inStream, outStream)) use { case (in, out) =>
      transfer(in, out)
    }

  def unpack(source: File, destination: File): IO[Long] =
    (for {
      inStream  <- gzipInputStream(source)
      outStream <- outputStream(destination)
    } yield (inStream, outStream)).use { case (in, out) =>
      transfer(in, out)
    }

  def writeToCsv(data: List[String], destination: File): IO[Long] =
    ioStreams(data.mkString("\n"), destination).use { case (in, out) =>
      transfer(in, out)
    }

  private def transfer(origin: InputStream, destination: OutputStream): IO[Long] =
    for {
      buffer <- IO(new Array[Byte](1024 * 10)) // Allocated only when the IO is evaluated
      total  <- transmit(origin, destination, buffer, 0L)
    } yield total

  private def transmit(origin: InputStream, destination: OutputStream, buffer: Array[Byte], acc: Long): IO[Long] =
    for {
      amount <- IO(origin.read(buffer, 0, buffer.length))
      count  <- if(amount > -1) IO(destination.write(buffer, 0, amount)) >> transmit(origin, destination, buffer, acc + amount)
                else IO.pure(acc) // End of read stream reached (by java.io.InputStream contract), nothing to write
    } yield count // Returns the actual amount of bytes transmitted

  private def fileInputStream(f: File): Resource[IO, FileInputStream] =
    Resource.make {
      IO(new FileInputStream(f)) //build
    } { inStream =>
      IO(inStream.close()).handleErrorWith(_ => IO.unit) //release
    }

  private def gzipInputStream(f: File): Resource[IO, GZIPInputStream] =
    Resource.make {
      IO(new GZIPInputStream(new FileInputStream(f)))
    } { inStream =>
      IO(inStream.close()).handleErrorWith(_ => IO.unit)
    }

  private def byteArrayInputStream(data: String): Resource[IO, ByteArrayInputStream] =
    Resource.make {
      IO(new ByteArrayInputStream(data.toCharArray.map(_.toByte)))
    } { inStream =>
      IO(inStream.close()).handleErrorWith(_ => IO.unit)
    }

  private def urlInputSteam(url: URL): Resource[IO, InputStream] =
    Resource.make {
      IO(url.openConnection().getInputStream)
    } { inStream =>
      IO(inStream.close()).handleErrorWith(_ => IO.unit)
    }

  private def outputStream(f: File): Resource[IO, FileOutputStream] =
    Resource.make {
      IO(new FileOutputStream(f))
    } { outStream =>
      IO(outStream.close()).handleErrorWith(_ => IO.unit)
    }

  private def ioStreams(data: String, out: File): Resource[IO, (InputStream, OutputStream)] =
    for {
      inStream  <- byteArrayInputStream(data)
      outStream <- outputStream(out)
    } yield (inStream, outStream)
}
