package io.listery;

import org.apache.hadoop.conf.Configuration;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

// store A
// store B
// store C

public class Application {
  public static void main(String[] args) {
    SparkSession sparkSession =
        SparkSession.builder().appName("listery-main").master("local[*]").getOrCreate();
    Configuration fsConfig = sparkSession.sparkContext().hadoopConfiguration();
    fsConfig.set("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem");
    fsConfig.set("fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS");
    fsConfig.set("google.cloud.auth.service.account.enable", "true");
    fsConfig.set(
        "google.cloud.auth.service.account.json.keyfile",
        "/home/dad/workspace/nithish_GCP.json");


    // Reading data from GCS
    Dataset<Row> storeA = sparkSession.read()
            .option("header",true)
            .csv("gs://listery-datalake/raw_data/2020-06-16/Store_A.csv");
    Dataset<Row> storeB = sparkSession.read()
            .option("header",true)
            .json("gs://listery-datalake/raw_data/2020-06-16/Store_B.json");
    Dataset<Row> storeC = sparkSession.read()
            .option("header",true)
            .csv("gs://listery-datalake/raw_data/2020-06-16/Store_C.csv");

    // transforming columns names to match those of the canonical one
    storeA = storeA.withColumnRenamed("asin", "id")
                    .withColumnRenamed("title","product")
                    .withColumnRenamed("imUrl","imgUrl")
                    .withColumnRenamed("categories","category");
    storeC = storeC.withColumnRenamed("product_id", "id")
                    .withColumnRenamed("name","product")
                    .withColumnRenamed("img_url","imgUrl");


    // union of files
    Dataset<Row> result = storeA.union(storeC);

    // writing as parquet to GCS
    result.write().parquet("gs://listery-datalake/canonical_data/test.parquet");

  }
}
