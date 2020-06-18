package io.listery;

import org.apache.spark.sql.*;

import java.text.ParseException;
import java.util.Optional;

public class PriceDiff {
  private final SparkSession sparkSession;

  PriceDiff(SparkSession sparkSession) {
    this.sparkSession = sparkSession;
  }

  void alertUserOnPriceChange(Optional<String> dateToProcess) throws ParseException {
    String today = dateToProcess.orElse(Utils.todayString());
    String yesterday = Utils.yesterdayString(today);

    Dataset<Row> userList =
        sparkSession
            .read()
            .option("mode", "DROPMALFORMED")
            .option("header", true)
            .csv("gs://listery-datalake/raw_data/wishlist.csv")
            .selectExpr("user_id", "store", "product_id as id");

    Dataset<Row> yesterdayPrice = getPriceData(yesterday);
    Dataset<Row> todayPrice = getPriceData(today);

    Dataset<Row> priceDrop =
        todayPrice
            .as("tp")
            .join(yesterdayPrice.as("yp"))
            .where("tp.price < yp.price and tp.id = yp.id and tp.store = yp.store")
            .selectExpr(
                "tp.id",
                "tp.store",
                "100*((tp.price - yp.price)/yp.price) as percentage_drop",
                "tp.price as price_today");
    Dataset<Row> alerts =
        userList
            .as("u")
            .join(priceDrop.as("p"))
            .where("u.id = p.id")
            .selectExpr("u.*", "p.percentage_drop", "p.price_today")
            .withColumn("date", functions$.MODULE$.lit(today));
    alerts.write().mode(SaveMode.Overwrite).partitionBy("date").parquet(Constants.PRICE_ALERT);
  }

  private Dataset<Row> getPriceData(String yesterday) {
    return sparkSession
        .read()
        .parquet(Constants.PRODUCTS)
        .where("date = '" + yesterday + "'")
        .where("price IS NOT NULL")
        .select("id", "store", "price");
  }
}
