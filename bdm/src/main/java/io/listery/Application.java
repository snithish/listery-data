package io.listery;

import org.apache.hadoop.conf.Configuration;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions$;

import java.text.ParseException;
import java.util.Arrays;
import java.util.Optional;

public class Application {
  public static void main(String[] args) throws ParseException {
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
    sparkSession.conf().set("spark.sql.sources.partitionOverwriteMode", "dynamic");

    switch (args[3]) {
      case "integration":
        {
          Optional<String> dateToProcess = Optional.empty();
          if (args.length == 5) {
            dateToProcess = Optional.of(args[4]);
          }
          new PhysicalIntegration(sparkSession).integrate(dateToProcess);
          break;
        }
      case "offerIntegration":
        {
          Optional<String> dateToProcess = Optional.empty();
          if (args.length == 5) {
            dateToProcess = Optional.of(args[4]);
          }
          new OffersIntegration(sparkSession).integrate(dateToProcess);
          break;
        }
      case "priceDiff":
        {
          Optional<String> dateToProcess = Optional.empty();
          if (args.length == 5) {
            dateToProcess = Optional.of(args[4]);
          }
          new PriceDiff(sparkSession).alertUserOnPriceChange(dateToProcess);
          break;
        }
      default:
        System.out.println("Invalid application");
        System.exit(1);
    }

    //    test(sparkSession);
  }

  private static void test(SparkSession sparkSession) {
    Dataset<Row> text =
        sparkSession
            .read()
            .schema(StoreConfig.schemaForStore("Store_B"))
            .json("gs://listery-datalake/raw_data/demo.json");
    Dataset<Row> products1 =
        text.select(functions$.MODULE$.explode(text.col("products")).as("exploded"))
            .select("exploded.*");
    Dataset<Row> a =
        sparkSession
            .read()
            .option("header", true)
            .csv("gs://listery-datalake/raw_data/2020-06-16/Store_A.csv");
    Dataset<Row> c =
        sparkSession
            .read()
            .option("header", true)
            .csv("gs://listery-datalake/raw_data/2020-06-16/Store_C.csv");
    long numberOfLines = text.count();
    products1.show();
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
