import org.apache.spark.api.java.function.ForeachPartitionFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.*;
import redis.clients.jedis.Jedis;
import scala.collection.mutable.Seq;

import java.util.Iterator;

public class CacheInitializer {
    private final static String redisHost = "localhost";
    private final static int redisPort = 6379;

    private final static int initYear = 2021;
    private final static int initMonth = 9;

    /**
     * Initialize Lpi in Redis
     *
     * This method loads Lpi data from a column delimited file and insert
     *
     * @param spark SparkSession
     * @param dataPath File path of Lpi data
     * @param sep delimiter
     */
    private static void initLpi(SparkSession spark, String dataPath, String sep) {
        Jedis jedis = new Jedis(redisHost, redisPort);

        // Raw Lpi data schema
        StructType schema = new StructType()
                .add("buId", DataTypes.StringType, false)
                .add("piId", DataTypes.StringType, false);

        // Get pre-prepared Lpi data
        Dataset<Row> lpi = spark.read()
                .schema(schema)
                .option("delimiter", sep)
                .csv(dataPath)
                .groupBy("buId")
                .agg(functions.collect_set("piId").as("piIdSet"));

        // Clear old Lpi cache
        jedis.eval("return redis.call('del', 'defaultKey', unpack(redis.call('keys', ARGV[1])))",
                0, "Lpi:*");

        // Insert Lpi into Redis Set: Lpi:{buId}
        lpi.foreachPartition(new ForeachPartitionFunction<Row>() {
            @Override
            public void call(Iterator<Row> t) throws Exception {
                Jedis jedis = new Jedis(redisHost, redisPort);
                while (t.hasNext()) {
                    Row r = t.next();
                    scala.collection.Iterator<String> iter = r.<Seq<String>>getAs("piIdSet").iterator();
                    while (iter.hasNext()) {
                        String piId = iter.next().toString();
                        jedis.sadd("Lpi:" + r.<String>getAs("buId"), piId);
                    }
                }
            }
        });
    }

    /**
     * Initialize AllPi in Redis
     *
     * This method reads transaction data of current month and upload distinct Pi into Redis Set.
     *
     * @param spark SparkSession
     */
    private static void initAllPi(SparkSession spark) {
        Jedis jedis = new Jedis(redisHost, redisPort);

        // Load transactions table
        Dataset<Row> transactions = spark.read()
                .format("org.apache.spark.sql.redis")
                .option("spark.redis.host", redisHost)
                .option("spark.redis.port", redisPort)
                .option("table", "Transactions")
                .option("key.column", "id")
                .load();

        // Get data from initMonth and group by `buId`
        Dataset<Row> allPi = transactions.where(functions.year(functions.col("tDate")).equalTo(initYear)
                        .and(functions.month(functions.col("tDate")).equalTo(initMonth)))
                .groupBy("buId")
                .agg(functions.collect_set("piId").as("piIdSet"));

        // Clear old AllPi cache
        jedis.eval("return redis.call('del', 'defaultKey', unpack(redis.call('keys', ARGV[1])))",
                0, "AllPi:*");

        // Insert AllPi into Redis Set: AllPi:{buId}
        allPi.foreachPartition(new ForeachPartitionFunction<Row>() {
            @Override
            public void call(Iterator<Row> t) throws Exception {
                Jedis jedis = new Jedis(redisHost, redisPort);
                while (t.hasNext()) {
                    Row r = t.next();
                    scala.collection.Iterator<String> iter = r.<Seq<String>>getAs("piIdSet").iterator();
                    while (iter.hasNext()) {
                        String piId = iter.next().toString();
                        jedis.sadd("AllPi:" + r.<String>getAs("buId"), piId);
                    }
                }
            }
        });
    }

    /**
     * Initialize RecurPi in Redis
     *
     * This method reads transaction data of current month and upload RecurPi into Redis Set.
     *
     * @param spark SparkSession
     */
    private static void initRecurPi(SparkSession spark) {
        // Load transactions table
        Dataset<Row> transactions = spark.read()
                .format("org.apache.spark.sql.redis")
                .option("spark.redis.host", redisHost)
                .option("spark.redis.port", redisPort)
                .option("table", "Transactions")
                .option("key.column", "id")
                .load();

        // Get data from initMonth and group by `buId`
        Dataset<Row> allPi = transactions.where(functions.year(functions.col("tDate")).equalTo(initYear)
                        .and(functions.month(functions.col("tDate")).equalTo(initMonth)))
                .groupBy("buId")
                .agg(functions.collect_set("piId").as("piIdSet"));

        // From AllPi, for each piId, check if it's in Lpi. If so, insert it into Redis Set: RecurPi:{buId}
        allPi.foreachPartition(new ForeachPartitionFunction<Row>() {
            @Override
            public void call(Iterator<Row> t) throws Exception {
                Jedis jedis = new Jedis(redisHost, redisPort);
                while (t.hasNext()) {
                    Row r = t.next();
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
    }

    public static void main(String[] args) {
        SparkSession spark = SparkSession
                .builder()
                .master("local[*]")
                .appName("CacheInitialization")
                .getOrCreate();

        // Initialize Lpi based on pre-prepared data file
        initLpi(spark, "data/m9/cache/", "|");

        // Initialize AllPi
        initAllPi(spark);

        // Initialize RecurPi
        initRecurPi(spark);
    }
}
