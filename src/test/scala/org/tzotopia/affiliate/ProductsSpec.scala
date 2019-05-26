package org.tzotopia.affiliate

import org.scalatest.{FlatSpec, Matchers}

import Products.{groupProducts, transformToCsv}

class ProductsSpec extends FlatSpec with Matchers {
  val lookupKey: String = "a"

  "Grouping products" should "yield empty list if no rows are present" in {
    groupProducts(List.empty, lookupKey)() shouldBe List.empty
  }

  it should "yield empty list if only there are no values" in {
    groupProducts(List(Some(Map("a" -> "", "b" -> ""))), lookupKey)() should contain (Map("a" -> "", "b" -> ""))
  }

  //don't like this one.
  it should "apply no transformation if there are no duplicate values of a given key" in {
    groupProducts(List(Some(Map("a" -> "one")), Some(Map("a" -> "two"))) , lookupKey)() should contain allOf (
      Map("a" -> "one"), Map("a" -> "two")
    )
  }

  it should "group properties of different values by duplicate key" in {
    val input = List(
      Some(Map("a" -> "one", "b" -> "two", "c" -> "three")),
      Some(Map("a" -> "one", "b" -> "four", "c" -> "three")),
      Some(Map("a" -> "two", "b" -> "two", "c" -> "three"))
    )

    groupProducts(input, lookupKey)() should contain allOf (
      Map("a" -> "one", "b" -> "two|four", "c" -> "three"),
      Map("a" -> "two", "b" -> "two", "c" -> "three")
    )
  }

  "Grouped products" should "transform to csv with header information" in {
    val input = List(Map("a" -> "one", "b" -> "", "c" -> "two"), Map("a" -> "three", "b" -> "four", "c" -> "five"))

    transformToCsv(input) should contain allOf ("a,b,c", "one,,two", "three,four,five")
  }
}
