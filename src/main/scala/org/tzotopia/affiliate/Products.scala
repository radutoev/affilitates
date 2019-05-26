package org.tzotopia.affiliate

import java.io.File
import java.net.URL
import java.util.concurrent.Executors

import cats.effect.{ContextShift, IO}
import cats.implicits._
import fs2.{io, text}

import scala.concurrent.ExecutionContext

object Products {
  val blockingEc = ExecutionContext.fromExecutorService(Executors.newFixedThreadPool(4))
  implicit val cs: ContextShift[IO] = IO.contextShift(ExecutionContext.global)

  def processAffiliateResource(url: URL, uniqueColumn: String, joinOn: Char): IO[File] =
    for {
      _     <- IO.unit
//      url   = new URL("https://productdata.awin.com/datafeed/download/apikey/044eda50bbb9336dd8f8dc6c00552269/language/de/fid/24501/bid/64925,63233,50897,64961,64997,65353,51473,51525,51583,65387,65439,51863,63263,65511,64927,65659,65713,65715,53373,53383,65801,65841,64651,53829,65887,64999,65941,63329,66015,66033,63347,66219,66319,64717,64615,66505,56819,66599,56957,66639,63733,64339,63397,66955,66961,66963,66999,67003,58585,58715,59387,63453,67275,63461,63463,64995,65003,64993,67393,67425,67467,67485,67501,60957,61251,61267,61627,67687,63517,61891,61939,62041,64929,67907,62735,68021,68029,69467,70395,70749,71113,71301,71641,71965,72285,72401,72493,72519,72537,72571,72697,72793,72859,72893,72907/columns/aw_deep_link,product_name,aw_product_id,merchant_product_id,merchant_image_url,description,merchant_category,search_price,merchant_name,merchant_id,category_name,category_id,aw_image_url,currency,store_price,delivery_cost,merchant_deep_link,language,last_updated,display_price,data_feed_id,brand_name,brand_id,colour,product_short_description,specifications,condition,product_model,model_number,dimensions,keywords,promotional_text,product_type,commission_group,merchant_product_category_path,merchant_product_second_category,merchant_product_third_category,rrp_price,saving,savings_percent,base_price,base_price_amount,base_price_text,product_price_old,delivery_restrictions,delivery_weight,warranty,terms_of_contract,delivery_time,in_stock,stock_quantity,valid_from,valid_to,is_for_sale,web_offer,pre_order,stock_status,size_stock_status,size_stock_amount,merchant_thumb_url,large_image,alternate_image,aw_thumb_url,alternate_image_two,alternate_image_three,alternate_image_four,reviews,average_rating,rating,number_available,custom_1,custom_2,custom_3,custom_4,custom_5,custom_6,custom_7,custom_8,custom_9,ean,isbn,upc,mpn,parent_product_id,product_GTIN,basket_link/format/csv/delimiter/%2C/compression/gzip/adultcontent/1/")
//      gzip  = new File("D:\\downloads\\csv-euri\\test.gz")
//      orig  = new File("D:\\downloads\\csv-euri\\test.csv")
      orig = new File("D:\\downloads\\csv-euri\\test.csv")
//            _     <- Files.readFromUrl(url, gzip)
//            _     <- Files.unpack(gzip, orig)
      data  <- parseFile(orig, uniqueColumn, joinOn)
      dest  =  new File("D:\\downloads\\csv-euri\\test-generated.csv")
      count <- Files.writeToCsv(data, dest)
      _     <- IO(println(s"$count bytes copied from ${orig.getPath} to ${dest.getPath}"))
    } yield dest

  def parseFile(file: File, uniqueColumn: String, joinOn: Char): IO[List[String]] =
    io.file.readAll[IO](file.toPath, blockingEc, 4096)
      .through(text.utf8Decode)
      .through(Fs2Csv.parse(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)"))
      .noneTerminate
      .compile
      .toList
      //out of the stream world.
      .map(Products.groupProducts(_, uniqueColumn)(joinOn))
      .map(Products.transformToCsv)

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
