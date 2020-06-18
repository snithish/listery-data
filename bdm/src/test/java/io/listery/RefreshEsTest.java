package io.listery;

import avro.shaded.com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import junit.framework.TestCase;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.*;
import org.elasticsearch.spark.rdd.api.java.JavaEsSpark;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class RefreshEsTest extends TestCase {
  public void testName() {
    SparkSession t = SparkSession.builder().appName("test").master("local[*]").getOrCreate();
    JavaSparkContext js = new JavaSparkContext(t.sparkContext());
    SQLContext sqc = new SQLContext(js);
    // sample data
    List<String> data = new ArrayList<String>();
    data.add("dev, engg");
    data.add("karthik, engg");
    // DataFrame
    Dataset<Row> df = sqc.createDataset(data, Encoders.STRING()).toDF();
    df.printSchema();
    df.show();
    // Convert
    Dataset<Row> df1 =
        df.selectExpr("split(value, ',')[0] as name", "split(value, ',')[1] as degree");
    df1.printSchema();
    Dataset<Row> rowDataset = df1.selectExpr("to_json(struct(name, degree)) as coljson");
    rowDataset.show();
  }

  public void testName1() {
    SparkSession t = SparkSession.builder().appName("test").master("local[*]").getOrCreate();
    JavaSparkContext js = new JavaSparkContext(t.sparkContext());
    SQLContext sqc = new SQLContext(js);
    Map<String, ?> otp = ImmutableMap.of("iata", "OTP", "name", "Otopeni");
    Map<String, ?> jfk = ImmutableMap.of("iata", "JFK", "name", "JFK NYC");
    JavaPairRDD<?, ?> pairRdd =
        js.parallelizePairs(
            ImmutableList.of(
                new Tuple2<Object, Object>(1, otp), new Tuple2<Object, Object>(2, jfk)));
    JavaEsSpark.saveToEsWithMeta(pairRdd, "test/bdm");
  }
}
