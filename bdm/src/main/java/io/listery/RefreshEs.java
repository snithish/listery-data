package io.listery;

import com.google.common.collect.ImmutableList;
import org.apache.spark.sql.*;
import org.elasticsearch.spark.sql.api.java.JavaEsSparkSQL;
import scala.collection.JavaConversions;

import java.util.Optional;

public class RefreshEs {
  private final SparkSession sparkSession;

  public RefreshEs(SparkSession sparkSession) {
    this.sparkSession = sparkSession;
  }

  public void refresh(Optional<String> dateToProcess) {
    String today = dateToProcess.orElse(Utils.todayString());
    Dataset<Row> products =
        sparkSession.read().parquet(Constants.PRODUCTS).where("date = '" + today + "'");
    Dataset<Row> offers =
        sparkSession
            .read()
            .parquet(Constants.OFFERS)
            .where("date = '" + today + "'")
            .withColumnRenamed("end", "valid_until");
    Dataset<Row> integratedOffers =
        products
            .as("p")
            .join(
                offers.as("o"),
                JavaConversions.asScalaBuffer(ImmutableList.of("id", "store")),
                "left_outer")
            .selectExpr("p.*", "to_json(struct(o.offer, o.valid_until)) as offer");

    RelationalGroupedDataset groupedOffers =
        integratedOffers.groupBy(integratedOffers.col("id"), integratedOffers.col("store"));

    Dataset<Row> aggregatedOffers =
        groupedOffers
            .agg(
                functions$.MODULE$.first("title").as("product_name"),
                functions$.MODULE$.first("imgUrl").as("imgUrl"),
                functions$.MODULE$.first("price").as("price"),
                functions$.MODULE$.first("category").as("category"),
                functions$.MODULE$.first("brand").as("brand"),
                functions$.MODULE$.first("description").as("description"),
                functions$.MODULE$.collect_list("offer").as("offers"))
            .cache();
    JavaEsSparkSQL.saveToEs(aggregatedOffers, "listery-products");
  }
}
