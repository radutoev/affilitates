package org.tzotopia.affiliate

import java.io.File

import cats.effect.IO
import org.scalatest.{FlatSpec, Matchers}

class ProductsSpec extends FlatSpec with Matchers {
  private val lookupKey: String = "a"
  private val appConfig: AppConfig = new AppConfig {
    override def affiliateConfig(name: String): IO[Either[Throwable, AffiliateConfig]] = IO.fromEither(Left(new RuntimeException("Not used")))
    override def workdir: IO[File] = IO.pure(File.createTempFile("what", "ever"))
    override def outputDir: IO[File] = IO.pure(File.createTempFile("what", "ever2"))
  }
  private val products = new CsvProducts(appConfig)

  private val defaultGroupProducts: (List[Option[Map[String, String]]], String) => List[Map[String, String]] =
    (data: List[Option[Map[String, String]]], lookup: String) => products.groupProducts(data, lookup)()()

  "Grouping products" should "yield empty list if no rows are present" in {
    defaultGroupProducts(List.empty, lookupKey) shouldBe List.empty
  }

  it should "yield empty list if only there are no values" in {
    defaultGroupProducts(List(Some(Map("a" -> "", "b" -> ""))), lookupKey) should contain (Map("a" -> "", "b" -> ""))
  }

  //don't like this one.
  it should "apply no transformation if there are no duplicate values of a given key" in {
    defaultGroupProducts(List(Some(Map("a" -> "one")), Some(Map("a" -> "two"))) , lookupKey) should contain allOf (
      Map("a" -> "one"), Map("a" -> "two")
    )
  }

  it should "group properties of different values by duplicate key" in {
    val input = List(
      Some(Map("a" -> "one", "b" -> "two", "c" -> "three")),
      Some(Map("a" -> "one", "b" -> "four", "c" -> "three")),
      Some(Map("a" -> "two", "b" -> "two", "c" -> "three"))
    )

    defaultGroupProducts(input, lookupKey) should contain allOf (
      Map("a" -> "one", "b" -> "two|four", "c" -> "three"),
      Map("a" -> "two", "b" -> "two", "c" -> "three")
    )
  }

  it should "join only specified columns and discard the rest" in {
    val input = List(
      Some(Map("a" -> "one", "b" -> "two", "c" -> "three")),
      Some(Map("a" -> "one", "b" -> "four", "c" -> "five")),
      Some(Map("a" -> "two", "b" -> "two", "c" -> "three"))
    )

    products.groupProducts(input, lookupKey)(Vector("b"))() should contain allOf (
      Map("a" -> "one", "b" -> "two|four", "c" -> "three"),
      Map("a" -> "two", "b" -> "two", "c" -> "three")
    )
  }

  "Grouped products" should "transform to csv with header information" in {
    val input = List(Map("a" -> "one", "b" -> "", "c" -> "two"), Map("a" -> "three", "b" -> "four", "c" -> "five"))

    products.transformToCsv(input) should contain allOf ("a,b,c", "one,,two", "three,four,five")
  }

  "Column transformations" should "apply join operator if comma is present" in {
    val colValue: String = "44M,44 2/3M,42 2/3M,47 1/3M,43 1/3M,41 1/3M,46M,45 1/3M,42M"
    products.applyColumnTransformations(colValue, '|') should equal ("44M|44 2/3M|42 2/3M|47 1/3M|43 1/3M|41 1/3M|46M|45 1/3M|42M")
  }
}
