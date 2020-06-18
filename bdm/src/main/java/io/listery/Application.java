package io.listery;

import avro.shaded.com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions$;
import org.elasticsearch.spark.rdd.api.java.JavaEsSpark;
import scala.Tuple2;

import java.text.ParseException;
import java.util.Arrays;
import java.util.Map;
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
    esConfig(sparkSession);
    Optional<String> dateToProcess = Optional.empty();
    if (args.length == 5) {
      dateToProcess = Optional.of(args[4]);
    }
    //    esTest(sparkSession);

    subprogram(args, sparkSession, dateToProcess);

    //    test(sparkSession);
  }

  private static void esTest(SparkSession sparkSession) {
    JavaSparkContext js = new JavaSparkContext(sparkSession.sparkContext());
    Map<String, ?> otp = ImmutableMap.of("iata", "OTP", "name", "Otopeni");
    Map<String, ?> jfk = ImmutableMap.of("iata", "JFK", "name", "JFK NYC");
    JavaPairRDD<?, ?> pairRdd =
        js.parallelizePairs(
            ImmutableList.of(
                new Tuple2<Object, Object>(1, otp), new Tuple2<Object, Object>(2, jfk)));
    JavaEsSpark.saveToEsWithMeta(pairRdd, "test");
  }

  private static void subprogram(
      String[] args, SparkSession sparkSession, Optional<String> dateToProcess)
      throws ParseException {
    switch (args[3]) {
      case "integration":
        {
          new PhysicalIntegration(sparkSession).integrate(dateToProcess);
          break;
        }
      case "offerIntegration":
        {
          new OffersIntegration(sparkSession).integrate(dateToProcess);
          break;
        }
      case "priceDiff":
        {
          new PriceDiff(sparkSession).alertUserOnPriceChange(dateToProcess);
          break;
        }
      case "refreshEs":
        {
          new RefreshEs(sparkSession).refresh(dateToProcess);
          break;
        }
      default:
        System.out.println("Invalid application");
        System.exit(1);
    }
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

  private static void esConfig(SparkSession sparkSession) {
    SparkConf conf = sparkSession.sparkContext().conf();
    conf.set("es.nodes", "https://3026df4bb7bb4b10bf29328065079ad3.us-central1.gcp.cloud.es.io");
    conf.set("es.port", "9243");
    conf.set("es.nodes.wan.only", "true");
    conf.set("es.net.http.auth.user", "elastic");
    conf.set("es.index.auto.create", "true");
    conf.set("es.net.http.auth.pass", "ITnOziHCzUkp6BroWEGJKxCZ");
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
