package io.listery;

import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.Optional;

public class OffersIntegration {
  private final SparkSession sparkSession;

  OffersIntegration(SparkSession sparkSession) {
    this.sparkSession = sparkSession;
  }

  void integrate(Optional<String> dateToProcess) {
    String today = dateToProcess.orElse(Utils.todayString());
    //    product_id,store,offer,start,end
    StructType offer =
        DataTypes.createStructType(
            new StructField[] {
              DataTypes.createStructField("product_id", DataTypes.StringType, false),
              DataTypes.createStructField("store", DataTypes.StringType, false),
              DataTypes.createStructField("offer", DataTypes.StringType, false),
              DataTypes.createStructField("start", DataTypes.DateType, false),
              DataTypes.createStructField("end", DataTypes.DateType, false),
            });
    Dataset<Row> offers =
        sparkSession
            .read()
            .option("header", "true")
            .schema(offer)
            .csv("gs://listery-datalake/raw_data/offers.csv");
    Dataset<Row> offersValidToday =
        offers
            .where("'" + today + "' between start and end")
            .withColumn("date", functions$.MODULE$.lit(today))
            .withColumnRenamed("product_id", "id");
    offersValidToday.write().mode(SaveMode.Overwrite).partitionBy("date").parquet(Constants.OFFERS);
  }
}
