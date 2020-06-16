package io.listery;

import org.apache.hadoop.conf.Configuration;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

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
        "/home/snithish/Downloads/upc-bdm-9c2f9309ef03.json");

    Dataset<Row> text = sparkSession.read().text("gs://listery-datalake/raw_data/rose.txt");
    long numberOflines = text.count();
    System.out.println("Number of lines in file: " + numberOflines);
  }
}
