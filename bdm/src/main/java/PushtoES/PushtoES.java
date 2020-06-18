package PushtoES;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Map;
import org.elasticsearch.spark.rdd.api.java.JavaEsSpark;

public class PushtoES {

    public static void push() {

        SparkConf conf = new SparkConf().setAppName("Listery-ES").setMaster("local[*]");
        conf.set("es.nodes", "https://3026df4bb7bb4b10bf29328065079ad3.us-central1.gcp.cloud.es.io");
        conf.set("es.port", "9243");
        conf.set("es.nodes.wan.only", "true");
        conf.set("es.net.http.auth.user", "elastic");
        conf.set("es.net.http.auth.pass", "ITnOziHCzUkp6BroWEGJKxCZ");
        JavaSparkContext jsc = new JavaSparkContext(conf);


        Map<String, ?> numbers = ImmutableMap.of("one",1,"two",2);
        Map<String, ?> airports = ImmutableMap.of("OTP", "Otopeni", "SFO", "San Fran");
        JavaRDD<Map<String, ?>> javaRDD = jsc.parallelize(ImmutableList.of(numbers, airports));
        JavaEsSpark.saveToEs(javaRDD, "spark");
    }
}
