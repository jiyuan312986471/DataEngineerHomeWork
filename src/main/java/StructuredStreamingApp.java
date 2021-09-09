import org.apache.spark.api.java.function.VoidFunction2;
import org.apache.spark.sql.*;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.types.*;

public class StructuredStreamingApp {
    /**
     * Streaming App handling incoming transaction data.
     *
     * For demo, App receives data from file source and sink to Redis DB
     * For production, App receives data from Kafka source and sink to Impala
     */

    private final static String redisHost = "localhost";
    private final static int redisPort = 6379;

    public static void main(String[] args) throws StreamingQueryException {
        SparkSession spark = SparkSession
                .builder()
                .master("local[*]")
                .appName("StreamingApp")
                .getOrCreate();

        StructType schema = new StructType()
                .add("id", DataTypes.StringType, false)
                .add("amt", DataTypes.IntegerType, false)
                .add("tDate", DataTypes.TimestampType, false)
                .add("buId", DataTypes.StringType, false)
                .add("piId", DataTypes.StringType, false);

        /*
        Dataset representing input stream

        Using file source for demo, Kafka source should be used in case of production.

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
                .csv("data/m9/streaming/");

        /*
        Streaming query and save to sink

        Using Redis for demo, Impala should be used in production.
         */
        StreamingQuery queryRows = rows.writeStream()
                .outputMode(OutputMode.Update())
                .foreachBatch((VoidFunction2<Dataset<Row>, Long>) (batchRows, batchId) ->
                        batchRows.write()
                                .format("org.apache.spark.sql.redis")
                                .option("spark.redis.host", redisHost)
                                .option("spark.redis.port", redisPort)
                                .option("table", "Transactions")
                                .option("key.column", "id")
                                .mode(SaveMode.Append)
                                .save()
                )
                .start();
        queryRows.awaitTermination();
    }
}
