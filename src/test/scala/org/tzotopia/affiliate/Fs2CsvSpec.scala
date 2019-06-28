package org.tzotopia.affiliate

import fs2.{Fallible, Stream}
import org.scalatest.{EitherValues, FlatSpec, Matchers}
import org.tzotopia.affiliate.Fs2Csv.{Columns, HeaderSizeMismatch}

class Fs2CsvSpec extends FlatSpec with Matchers with EitherValues {
  behavior of "parse"

//  it should "associate row values with headers" in {
//    val csv =
//      """a,b,c
//        |1,2,3
//        |4,5,6
//      """.stripMargin
//    val csvMaps = Stream
//      .emit(csv)
//      .covary[Fallible]
//      .through(Fs2Csv.parse(",")(Option.empty[Columns]))
//      .toVector
//      .right
//      .value
//
//    csvMaps shouldEqual Vector(
//      Map("a" -> ("1", 0), "b" -> ("2", 1), "c" -> ("3", 2)),
//      Map("a" -> ("4", 0), "b" -> ("5", 0), "c" -> ("6", 0))
//    )
//  }

//  it should "error on bad row alignment" in {
//    val csv =
//      """a,b,c
//        |1,2,3
//        |4,5
//      """.stripMargin
//
//    a[HeaderSizeMismatch] shouldBe thrownBy {
//      Stream
//        .emit(csv)
//        .covary[Fallible]
//        .through(Fs2Csv.parse(","))
//        .toVector
//        .right
//        .value
//    }
//  }

  it should "handle rows with leading empty columns" in {
    val rows =
      """item-name	item-description	listing-id	seller-sku	price	quantity	open-date	image-url	item-is-marketplace	product-id-type	zshop-shipping-fee	item-note	item-condition	zshop-category1	zshop-browse-path	zshop-storefront-feature	asin1	asin2	asin3	will-ship-internationally	expedited-shipping	zshop-boldface	product-id	bid-for-featured-placement	add-delete	pending-quantity	fulfillment-channel	merchant-shipping-group	status
        |		0ABCDHF8GY	D-ABCDL-T01	39.95		2013-01-07 07:35:42 PST		y	1							B0ABCDDUO						B0ABCDDUO				AMAZON_NA	Migrated Template	Active
      """.stripMargin
    val result =
      Stream
        .emit(rows)
        .covary[Fallible]
        .through(Fs2Csv.parse("\t")(Option.empty[Columns]))
        .toVector
        .right
        .value
    result should have length 1
  }

  it should "handle rows with trailing empty columns" in {
    val rows =
      """amazon-order-id	merchant-order-id	purchase-date	last-updated-date	order-status	fulfillment-channel	sales-channel	order-channel	url	ship-service-level	product-name	sku	asin	item-status	quantity	currency	item-price	item-tax	shipping-price	shipping-tax	gift-wrap-price	gift-wrap-tax	item-promotion-discount	ship-promotion-discount	ship-city	ship-state	ship-postal-code	ship-country	promotion-ids	is-business-order	purchase-order-number	price-designation
        |scrubbed	scrubbed	2017-06-17T08:04:21+00:00	2017-06-18T22:07:59+00:00	Shipped	Amazon	Amazon.it			SecondDay	scrubbed	scrubbed	scrubbed	Cancelled	1	EUR	89.95								TOWNNAME	GA	30041-8435	US		false		""".stripMargin
    val result =
      Stream
        .emit(rows)
        .covary[Fallible]
        .through(Fs2Csv.parse("\t")(Option.empty[Columns]))
        .toVector
        .right
        .value
    result should have length 1
  }

  it should "trim header columns" in {
    val csv =
      """a ,b,c
        |1,2,3
      """.stripMargin
    val csvMap = Stream
      .emit(csv)
      .covary[Fallible]
      .through(Fs2Csv.parse(",")(Option.empty[Columns]))
      .toVector
      .right
      .value
      .head

    csvMap.keySet should contain("a")
    csvMap.keySet shouldNot contain("a ")
  }
}


