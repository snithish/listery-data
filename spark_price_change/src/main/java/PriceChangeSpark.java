import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class PriceChangeSpark {
	
	public static void detectPriceChange(SparkSession spark) {

		Dataset<Row> user_list = spark.read()
				.option("mode", "DROPMALFORMED").option("header",true)
				.csv("src/main/resources/sample_user_db.csv");


		Dataset<Row> yesterday = spark.read()
				.option("mode", "DROPMALFORMED").option("header",true)
				.csv("src/main/resources/sample_price_1.csv").select("asin","price");

		Dataset<Row> today = spark.read()
				.option("mode", "DROPMALFORMED").option("header",true)
				.csv("src/main/resources/sample_price_2.csv").select("asin", "price");

		today = today.filter("price IS NOT NULL");
		yesterday = yesterday.filter("price IS NOT NULL");

		yesterday = yesterday.withColumnRenamed("price", "old_price");
		yesterday = yesterday.withColumnRenamed("asin", "old_asin");
		Dataset<Row> joined = today.join(yesterday, today.col("asin").equalTo(yesterday.col("old_asin")),"inner");
		Dataset<Row> final_out = joined.filter(joined.col("price").notEqual(joined.col("old_price")));

		joined.show();
		Dataset<Row> final_df = final_out.select("asin", "price", "old_price");
		Dataset<Row> result = final_df.withColumn("price_diff", final_df.col("price").$minus(final_df.col("old_price")));

		result.show();
		Dataset<Row> joined_with_user = user_list.join(result, user_list.col("asin").equalTo(result.col("asin")));
		joined_with_user.show();

	}
}

