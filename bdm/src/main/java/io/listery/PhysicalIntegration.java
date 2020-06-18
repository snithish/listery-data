package io.listery;

import com.google.common.collect.ImmutableList;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import scala.collection.JavaConversions;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

public class PhysicalIntegration {
  private static final List<String> STORES =
      ImmutableList.of("Store_A.csv", "Store_C.csv", "Store_B.json");
  private final SparkSession sparkSession;

  PhysicalIntegration(SparkSession sparkSession) {
    this.sparkSession = sparkSession;
  }

  void integrate(Optional<String> dateToProcess) {
    String today = dateToProcess.orElse(Utils.todayString());
    List<Dataset<Row>> dfs =
        STORES.stream()
            .map(
                store -> {
                  String filepath = "gs://listery-datalake/raw_data/" + today + "/" + store;
                  Dataset<Row> data = getDataFrame(store, filepath);
                  data = renameColumns(store, data);
                  data = dropExtraColumns(data);
                  data = addMissingCanonicalColumns(data);
                  data = data.withColumn("store", functions$.MODULE$.lit(store.split("\\.")[0]));
                  data = data.withColumn("date", functions$.MODULE$.lit(today));
                  List<String> canonicalColumns =
                      ImmutableList.<String>builder()
                          .addAll(StoreConfig.canonicalColumns())
                          .add("store")
                          .add("date")
                          .build();
                  return data.selectExpr(JavaConversions.asScalaBuffer(canonicalColumns));
                })
            .collect(Collectors.toList());
    Dataset<Row> storesUnionedData = dfs.stream().skip(1).reduce(dfs.get(0), Dataset::unionAll);
    storesUnionedData
        .write()
        .mode(SaveMode.Overwrite)
        .partitionBy("date")
        .parquet(Constants.PRODUCTS);
  }

  private Dataset<Row> addMissingCanonicalColumns(Dataset<Row> data) {
    List<String> columns = Arrays.asList(data.columns());
    for (String canonicalColumn : StoreConfig.canonicalColumns()) {
      if (columns.contains(canonicalColumn)) {
        continue;
      }
      data =
          data.withColumn(canonicalColumn, functions$.MODULE$.lit(null).cast(DataTypes.StringType));
    }
    return data;
  }

  private Dataset<Row> renameColumns(String store, Dataset<Row> data) {
    String[] columns = data.columns();
    Map<String, String> columnRemapping = StoreConfig.storeColumnMapping(store);
    for (String column : columns) {
      if (!columnRemapping.containsKey(column)) {
        continue;
      }
      data = data.withColumnRenamed(column, columnRemapping.get(column));
    }
    return data;
  }

  private Dataset<Row> dropExtraColumns(Dataset<Row> data) {
    String[] renamedColumns = data.columns();
    for (String column : renamedColumns) {
      if (StoreConfig.canonicalColumns().contains(column)) {
        continue;
      }
      data = data.drop(column);
    }
    return data;
  }

  private Dataset<Row> getDataFrame(String store, String filepath) {
    DataFrameReader dataFrameReader = sparkSession.read().schema(StoreConfig.schemaForStore(store));
    if (filepath.endsWith(".json")) {
      Dataset<Row> text = dataFrameReader.json(filepath);
      return text.select(functions$.MODULE$.explode(text.col("products")).as("exploded"))
          .select("exploded.*");
    }
    return dataFrameReader.option("header", true).csv(filepath);
  }
}
