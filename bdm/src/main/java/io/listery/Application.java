package io.listery;

import org.apache.hadoop.conf.Configuration;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.Arrays;

public class Application {
  public static void main(String[] args) {
    System.out.println("usage: --local [boolean true or false] --subprogram [integration]");
    System.out.println("Invoking with :" + Arrays.toString(args));
    if (args.length < 4) {
      System.out.println("Failure, booting out!!");
      System.exit(1);
    }
    SparkSession.Builder builder = SparkSession.builder().appName("listery-main");
    boolean runningInLocalMode = args[1].equals("true");
    if (runningInLocalMode) {
      builder.master("local[*]");
    }
    SparkSession sparkSession = builder.getOrCreate();
    if (runningInLocalMode) {
      localConfig(sparkSession);
    }

    Dataset<Row> text = sparkSession.read().text("gs://listery-datalake/raw_data/rose.txt");
    long numberOfLines = text.count();
    System.out.println("Number of lines in file: " + numberOfLines);
  }

  private static void localConfig(SparkSession sparkSession) {
    Configuration fsConfig = sparkSession.sparkContext().hadoopConfiguration();
    fsConfig.set("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem");
    fsConfig.set("fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS");
    fsConfig.set("google.cloud.auth.service.account.enable", "true");
    fsConfig.set(
        "google.cloud.auth.service.account.json.keyfile",
        "/home/snithish/Downloads/upc-bdm-9c2f9309ef03.json");
  }
}
