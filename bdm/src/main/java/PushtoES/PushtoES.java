package PushtoES;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Map;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.elasticsearch.spark.rdd.api.java.JavaEsSpark;

import static org.apache.spark.sql.functions.*;

public class PushtoES {

    public static void pushOffers(SparkSession spark) {

        //configuration in the main file must be as follows:

//        SparkConf conf = new SparkConf().setAppName("Listery-ES").setMaster("local[*]");
//        conf.set("es.nodes", "https://3026df4bb7bb4b10bf29328065079ad3.us-central1.gcp.cloud.es.io");
//        conf.set("es.port", "9243");
//        conf.set("es.nodes.wan.only", "true");
//        conf.set("es.net.http.auth.user", "elastic");
//        conf.set("es.net.http.auth.pass", "ITnOziHCzUkp6BroWEGJKxCZ");
//        conf.set("spark.sql.warehouse.dir", "file:\\\\\\D:\\upc\\bdm\\streaminglab\\Lab06-Java_project\\src\\main\\resources");
//
//        SparkSession.Builder builder = SparkSession.builder();
//        SparkSession spark = builder.config(conf).getOrCreate();

        String filepath = "file:\\\\\\D:\\upc\\bdm\\streaminglab\\Lab06-Java_project\\src\\main\\resources\\offers.parquet";
        Dataset<Row> offer_list = spark.read().parquet(filepath);
        //converting row into json
        offer_list = offer_list.select(to_json(struct(col("product_id"),col("store"), col("offer"), col("start"), col("end"))).as("offers"), col("product_id"));
        //grouping
        offer_list = offer_list.groupBy("product_id").agg(collect_list("offers").as("offers"));

        org.elasticsearch.spark.sql.EsSparkSQL.saveToEs(offer_list,"offers");


    }

    public static void pushProducts(SparkSession spark){
        String filepath = "file:\\\\\\D:\\upc\\bdm\\streaminglab\\Lab06-Java_project\\src\\main\\resources\\offers.parquet";
        Dataset<Row> product_list = spark.read().parquet(filepath);

        org.elasticsearch.spark.sql.EsSparkSQL.saveToEs(product_list,"products");
    }
}
