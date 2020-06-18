package io.listery;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

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

    //    Dataset<Row> user_list = sparkSession.read()
    //            .option("mode", "DROPMALFORMED").option("header",true)
    //            .csv("src/main/resources/sample_user_db.csv");

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
    priceDrop.show();
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
