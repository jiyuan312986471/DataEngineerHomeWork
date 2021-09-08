import org.apache.spark.api.java.function.VoidFunction2;
import org.apache.spark.sql.*;
import org.apache.spark.sql.functions.*;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.types.*;

public class StructuredStreamingApp {
    public static void main(String[] args) throws StreamingQueryException {
        SparkSession spark = SparkSession
                .builder()
                .master("local[*]")
                .appName("StreamingApp")
                .getOrCreate();

        StructType schema = new StructType()
                .add("id", DataTypes.StringType, false)
                .add("amt", DataTypes.IntegerType, false)
                .add("tDate", DataTypes.DateType, false)
                .add("buId", DataTypes.StringType, false)
                .add("piId", DataTypes.StringType, false);

        /*
        Dataset representing input stream

        Using file source for demo, Kafka source should be used for this project case.

        Dataset<Row> lines = spark
                .readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", "host1:port1,host2:port2")
                .option("subscribe", "topic1,topic2")
                .load()
                .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)");
        Dataset<Row> rows = lines.select(
                functions.split(functions.col("value"), "|").getItem(0).as("id"),
                functions.split(functions.col("value"), "|").getItem(1).as("amt"),
                functions.split(functions.col("value"), "|").getItem(2).as("tDate"),
                functions.split(functions.col("value"), "|").getItem(3).as("buId"),
                functions.split(functions.col("value"), "|").getItem(4).as("piId")
        );
         */
        Dataset<Row> rows = spark
                .readStream()
                .option("delimiter", "|")
                .schema(schema)
                .csv("data/");

        // Build streaming query and set Redis as output destination
        StreamingQuery query = rows.writeStream()
                .outputMode(OutputMode.Append())
                .foreachBatch((VoidFunction2<Dataset<Row>, Long>) (batchRows, batchId) ->
                        batchRows.write()
                                .format("org.apache.spark.sql.redis")
                                .option("spark.redis.host", "localhost")
                                .option("spark.redis.port", "6379")
                                .option("table", "Transactions")
                                .option("key.column", "id")
                                .save()
                )
                .start();
        query.awaitTermination();
    }
}
