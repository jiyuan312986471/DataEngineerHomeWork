import org.apache.spark.api.java.function.ForeachPartitionFunction;
import org.apache.spark.api.java.function.VoidFunction2;
import org.apache.spark.sql.*;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.types.*;
import redis.clients.jedis.Jedis;
import scala.collection.mutable.Seq;

import java.util.Iterator;

public class StructuredStreamingApp {
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
                .csv("data/m8/streaming/");

        // AllPi by business
        Dataset<Row> allPi = rows.groupBy("buId")
                .agg(functions.collect_set("piId").as("piIdSet"))
                .persist();

        // Update Redis cache: AllPi
        allPi.foreachPartition(new ForeachPartitionFunction<Row>() {
            @Override
            public void call(Iterator<Row> t) throws Exception {
                Jedis jedis = new Jedis(redisHost, redisPort);
                while (t.hasNext()) {
                    Row r = t.next();
                    Seq<String> piIdSet = r.<Seq<String>>getAs("piIdSet");
                    scala.collection.Iterator<String> iter = r.<Seq<String>>getAs("piIdSet").iterator();
                    while (iter.hasNext()) {
                        String piId = iter.next().toString();
                        jedis.sadd("AllPi:" + r.<String>getAs("buId"), piId);
                    }
                }
            }
        });

        // Update Redis cache: RecurPi
        allPi.foreachPartition(new ForeachPartitionFunction<Row>() {
            @Override
            public void call(Iterator<Row> t) throws Exception {
                Jedis jedis = new Jedis(redisHost, redisPort);
                while (t.hasNext()) {
                    Row r = t.next();
                    Seq<String> piIdSet = r.<Seq<String>>getAs("piIdSet");
                    scala.collection.Iterator<String> iter = r.<Seq<String>>getAs("piIdSet").iterator();
                    while (iter.hasNext()) {
                        String piId = iter.next().toString();
                        if (jedis.sismember("Lpi:" + r.<String>getAs("buId"), piId)) {
                            jedis.sadd("RecurPi:" + r.<String>getAs("buId"), piId);
                        }
                    }
                }
            }
        });

        // Unpersist AllPi to save memory
        allPi.unpersist();

        /*
        Build streaming query and save rows to sink

        Using Redis for demo, Impala should be used in production.
         */
        StreamingQuery query = rows.writeStream()
                .outputMode(OutputMode.Update())
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
