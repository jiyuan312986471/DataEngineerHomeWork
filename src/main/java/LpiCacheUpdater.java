import org.apache.spark.api.java.function.ForeachFunction;
import org.apache.spark.api.java.function.ForeachPartitionFunction;
import org.apache.spark.sql.*;
import redis.clients.jedis.Jedis;
import scala.collection.mutable.Seq;

import java.time.LocalDate;
import java.util.Iterator;

public class LpiCacheUpdater {
    private final static String redisHost = "localhost";
    private final static int redisPort = 6379;

    private final static int currentYear = LocalDate.now().getYear();
    private final static int currentMonth = LocalDate.now().getMonthValue();
    private final static int rcrYear = currentMonth == 1 ? currentYear - 1 : currentYear;
    private final static int rcrMonth = currentMonth == 1 ? 12 : currentMonth - 1;

    private final static int lpiStartYear = currentYear - 1;
    private final static int lpiStartMonth = currentMonth;
    private final static int lpiEndYear = rcrYear;
    private final static int lpiEndMonth = currentMonth;

    private static void updateLpi(SparkSession spark) {
        // Load transactions table
        Dataset<Row> trans = spark.read()
                .format("org.apache.spark.sql.redis")
                .option("spark.redis.host", redisHost)
                .option("spark.redis.port", redisPort)
                .option("table", "Transactions")
                .option("key.column", "id")
                .load();

        // Keep past 12 months data
        trans.createOrReplaceTempView("trans");
        String q = String.format("select * from trans where tDate >= '%04d-%02d-01' and tDate < '%04d-%02d-01'",
                lpiStartYear, lpiStartMonth, lpiEndYear, lpiEndMonth);
        Dataset<Row> trans1Y = spark.sql(q);
        Dataset<Row> lpi = trans1Y.groupBy("buId")
                .agg(functions.collect_set("piId").as("piIdSet"));

        // Clear old Lpi cache
        Jedis jedis = new Jedis(redisHost, redisPort);
        jedis.eval("return redis.call('del', 'defaultKey', unpack(redis.call('keys', ARGV[1])))",
                0, "Lpi:*");

        // Insert Lpi into Redis Set: Lpi:{buId}
        lpi.foreachPartition(new ForeachPartitionFunction<Row>() {
            @Override
            public void call(Iterator<Row> t) throws Exception {
                Jedis jedis = new Jedis(redisHost, redisPort);
                while (t.hasNext()) {
                    Row r = t.next();
                    Seq<String> piIdSet = r.<Seq<String>>getAs("piIdSet");
                    scala.collection.Iterator<String> iter = r.<Seq<String>>getAs("piIdSet").iterator();
                    while (iter.hasNext()) {
                        String piId = iter.next().toString();
                        jedis.sadd("Lpi:" + r.<String>getAs("buId"), piId);
                    }
                }
            }
        });
    }

    private static void updateAllPi(SparkSession spark) {
        // Load transactions table
        Dataset<Row> transactions = spark.read()
                .format("org.apache.spark.sql.redis")
                .option("spark.redis.host", redisHost)
                .option("spark.redis.port", redisPort)
                .option("table", "Transactions")
                .option("key.column", "id")
                .load();

        // Get data from current month and group by `buId`
        Dataset<Row> allPi = transactions.where(functions.year(functions.col("tDate")).equalTo(currentYear)
                        .and(functions.month(functions.col("tDate")).equalTo(currentMonth)))
                .groupBy("buId")
                .agg(functions.collect_set("piId").as("piIdSet"));

        // Clear old AllPi cache
        Jedis jedis = new Jedis(redisHost, redisPort);
        jedis.eval("return redis.call('del', 'defaultKey', unpack(redis.call('keys', ARGV[1])))",
                0, "AllPi:*");

        // Insert AllPi into Redis Set: AllPi:{buId}
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
    }

    private static void updateRecurPi(SparkSession spark) {
        // Load transactions table
        Dataset<Row> transactions = spark.read()
                .format("org.apache.spark.sql.redis")
                .option("spark.redis.host", redisHost)
                .option("spark.redis.port", redisPort)
                .option("table", "Transactions")
                .option("key.column", "id")
                .load();

        // Get data from initMonth and group by `buId`
        Dataset<Row> allPi = transactions.where(functions.year(functions.col("tDate")).equalTo(currentYear)
                        .and(functions.month(functions.col("tDate")).equalTo(currentMonth)))
                .groupBy("buId")
                .agg(functions.collect_set("piId").as("piIdSet"));

        // Clear old AllPi cache
        Jedis jedis = new Jedis(redisHost, redisPort);
        jedis.eval("return redis.call('del', 'defaultKey', unpack(redis.call('keys', ARGV[1])))",
                0, "RecurPi:*");

        // From AllPi, for each piId, check if it's in Lpi. If so, insert it into Redis Set: RecurPi:{buId}
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
    }

    private static void saveRCR(String buId, Jedis jedis) {
        // Check AllPi count first, no need to go further if 0 returned
        float cntAllPi = jedis.scard("AllPi:" + buId);
        if (cntAllPi == 0) {
            return;
        }

        float cntRecurPi = jedis.scard("RecurPi:" + buId);
        float rcr = cntRecurPi / cntAllPi;

        // Save rcr to Redis in the form of: Rcr:{buId} {yyyy}-{mm} {value}
        jedis.hset("Rcr:" + buId, String.format("%d-%d", rcrYear, rcrMonth), Float.toString(rcr));
    }

    private static void saveAllRCR(SparkSession spark) {
        // Load transactions table
        Dataset<Row> transactions = spark.read()
                .format("org.apache.spark.sql.redis")
                .option("spark.redis.host", redisHost)
                .option("spark.redis.port", redisPort)
                .option("table", "Transactions")
                .option("key.column", "id")
                .load();

        // Keep previous month data
        transactions.createOrReplaceTempView("trans");
        String q = String.format("select * from trans where tDate >= '%04d-%02d-01' and tDate < '%04d-%02d-01'",
                rcrYear, rcrMonth, currentYear, currentMonth);
        Dataset<Row> transLastMonth = spark.sql(q);

        // Get all distinct `buId` of previous month
        Dataset<Row> buIds = transLastMonth.select("buId").distinct();

        // Save RCR for each `buId`
        buIds.foreach(new ForeachFunction<Row>() {
            @Override
            public void call(Row row) throws Exception {
                Jedis jedis = new Jedis(redisHost, redisPort);
                String buId = row.getString(0);
                saveRCR(buId, jedis);
            }
        });
    }

    public static void updateMonthlyCache(SparkSession spark) {
        saveAllRCR(spark);
        updateLpi(spark);
        updateAllPi(spark);
        updateRecurPi(spark);
    }

    public static void main(String[] args) {
        SparkSession spark = SparkSession
                .builder()
                .master("local[*]")
                .appName("LpiCacheUpdate")
                .getOrCreate();
        updateMonthlyCache(spark);
    }
}
