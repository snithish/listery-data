package io.listery;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.List;
import java.util.Map;

public class StoreConfig {

  public static final String TITLE = "title";
  public static final String ID = "id";
  public static final String IMG_URL = "imgUrl";
  public static final String CATEGORY = "category";
  public static final String PRICE = "price";
  public static final String DESCRIPTION = "description";
  public static final String BRAND = "brand";

  static Map<String, String> storeColumnMapping(String store) {
    Map<String, String> storeAMapping =
        ImmutableMap.of("asin", ID, "title", TITLE, "imUrl", IMG_URL, "categories", CATEGORY);
    Map<String, String> storeCMapping =
        ImmutableMap.of("product_id", ID, "name", TITLE, "img_url", IMG_URL, "productBrand", BRAND);
    Map<String, String> storeBMapping =
        ImmutableMap.of("asin", ID, "title", TITLE, "imUrl", IMG_URL);

    return ImmutableMap.of(
            "Store_A.csv",
            storeAMapping,
            "Store_C.csv",
            storeCMapping,
            "Store_B.json",
            storeBMapping)
        .get(store);
  }

  static StructType schemaForStore(String store) {
    StructType storeA =
        DataTypes.createStructType(
            new StructField[] {
              DataTypes.createStructField("asin", DataTypes.StringType, false),
              DataTypes.createStructField("title", DataTypes.StringType, false),
              DataTypes.createStructField("price", DataTypes.DoubleType, false),
              DataTypes.createStructField("imUrl", DataTypes.StringType, false),
              DataTypes.createStructField("brand", DataTypes.StringType, false),
              DataTypes.createStructField("categories", DataTypes.StringType, false),
              DataTypes.createStructField("description", DataTypes.StringType, false),
            });
    StructType storeC =
        DataTypes.createStructType(
            new StructField[] {
              DataTypes.createStructField("product_id", DataTypes.StringType, false),
              DataTypes.createStructField("name", DataTypes.StringType, false),
              DataTypes.createStructField("price", DataTypes.DoubleType, false),
              DataTypes.createStructField("img_url", DataTypes.StringType, false),
              DataTypes.createStructField("description", DataTypes.StringType, false),
              DataTypes.createStructField("productBrand", DataTypes.StringType, false),
            });
    StructType storeBProducts =
        DataTypes.createStructType(
            new StructField[] {
              DataTypes.createStructField("asin", DataTypes.StringType, false),
              DataTypes.createStructField("title", DataTypes.StringType, true),
              DataTypes.createStructField("price", DataTypes.DoubleType, true),
              DataTypes.createStructField("imUrl", DataTypes.StringType, true),
              DataTypes.createStructField("description", DataTypes.StringType, true),
            });

    StructType storeB =
        DataTypes.createStructType(
            new StructField[] {
              DataTypes.createStructField(
                  "products", DataTypes.createArrayType(storeBProducts), false)
            });
    Map<String, StructType> config =
        ImmutableMap.of("Store_B.json", storeB, "Store_A.csv", storeA, "Store_C.csv", storeC);
    return config.get(store);
  }

  static List<String> canonicalColumns() {
    return ImmutableList.of(TITLE, ID, IMG_URL, CATEGORY, PRICE, BRAND, DESCRIPTION);
  }
}
