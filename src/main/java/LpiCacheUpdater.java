import org.apache.spark.api.java.function.ForeachFunction;
import org.apache.spark.api.java.function.ForeachPartitionFunction;
import org.apache.spark.sql.*;
import redis.clients.jedis.Jedis;
import scala.collection.mutable.Seq;

import java.time.LocalDate;
import java.util.Iterator;

/**
 * Update Lpi related cache
 *
 * `updateMonthly*` methods should be called at the beginning of a new calendar month, when we need to sink recurrent
 * customer ratio of previous month into DB then clear and recalculate Lpi, AllPi, RecurPi.
 *
 * `updateDaily*` methods should be triggered periodically (daily) by a scheduler to update AllPi and RecurPi in Redis
 * for current month.
 */
public class LpiCacheUpdater {
    private final static String redisHost = "localhost";
    private final static int redisPort = 6379;

    private final static int currentYear = LocalDate.now().getYear();
    private final static int currentMonth = LocalDate.now().getMonthValue();
    private final static int currentDay = LocalDate.now().getDayOfMonth();
    private final static int previousMonth_yyyy = currentMonth == 1 ? currentYear - 1 : currentYear;
    private final static int previousMonth_mm = currentMonth == 1 ? 12 : currentMonth - 1;

    /**
     * Update Lpi for previous month
     *
     * Clear the `Lpi:*` keys and update it with recalculated Lpi from past 12 months.
     *
     * @param trans1Y transaction data for past 12 months
     */
    private static void updateMonthlyLpi(Dataset<Row> trans1Y) {
        Jedis jedis = new Jedis(redisHost, redisPort);

        Dataset<Row> lpi = trans1Y.groupBy("buId")
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
     * Update AllPi in Redis
     *
     * @param allPi Dataset of columns `buId`, `piIdSet`, this Dataset should be created from a `groupBy` column
     *              expression on transaction data.
     */
    private static void updateAllPi(Dataset<Row> allPi) {
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
     * Update AllPi in Redis using transaction daily data
     *
     * @param dailyTrans transaction daily data
     */
    private static void updateDailyAllPi(Dataset<Row> dailyTrans) {
        Dataset<Row> allPi = dailyTrans.groupBy("buId")
                .agg(functions.collect_set("piId").as("piIdSet"));

        updateAllPi(allPi);
    }

    /**
     * Clear and Update AllPi in Redis
     *
     * Clear `AllPi:*` keys and update it with current month data
     *
     * @param transM transaction data for current month
     */
    private static void updateMonthlyAllPi(Dataset<Row> transM) {
        Jedis jedis = new Jedis(redisHost, redisPort);

        // Get data from current month and group by `buId`
        Dataset<Row> allPi = transM.groupBy("buId")
                .agg(functions.collect_set("piId").as("piIdSet"));

        // Clear old AllPi cache
        jedis.eval("return redis.call('del', 'defaultKey', unpack(redis.call('keys', ARGV[1])))",
                0, "AllPi:*");

        updateAllPi(allPi);
    }

    /**
     * Update RecurPi in Redis
     *
     * @param allPi Dataset of columns `buId`, `piIdSet`, this Dataset should be created from a `groupBy` column
     *              expression on transaction data.
     */
    private static void updateRecurPi(Dataset<Row> allPi) {
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

    /**
     * Update RecurPi in Redis using transaction daily data
     *
     * @param dailyTrans transaction daily data
     */
    private static void updateDailyRecurPi(Dataset<Row> dailyTrans) {
        Dataset<Row> allPi = dailyTrans.groupBy("buId")
                .agg(functions.collect_set("piId").as("piIdSet"));

        updateRecurPi(allPi);
    }

    /**
     * Clear and Update RecurPi in Redis
     *
     * Clear `RecurPi:*` keys and update it with current month data
     *
     * @param transM transaction data for current month
     */
    private static void updateMonthlyRecurPi (Dataset<Row> transM) {
        Jedis jedis = new Jedis(redisHost, redisPort);

        // Get data from initMonth and group by `buId`
        Dataset<Row> allPi = transM.groupBy("buId")
                .agg(functions.collect_set("piId").as("piIdSet"));

        // Clear old AllPi cache
        jedis.eval("return redis.call('del', 'defaultKey', unpack(redis.call('keys', ARGV[1])))",
                0, "RecurPi:*");

        updateRecurPi(allPi);
    }

    /**
     * Get RCR and save to Redis Hash
     *
     * When monthly updating Lpi caches, this method should always be called first, otherwise AllPi and RecurPi will be
     * lost (it's still possible to recalculate from DB sink, which is however time-consuming).
     *
     * @param buId Business id
     * @param jedis Jedis instance, Redis connector
     */
    private static void saveRCR(String buId, Jedis jedis) {
        float rcr = RecurrentCustomerRatioCalculator.calculateRCR(buId);

        // Save rcr to Redis in the form of: Rcr:{buId} {yyyy}-{mm} {value}
        jedis.hset("Rcr:" + buId, String.format("%d-%d", previousMonth_yyyy, previousMonth_mm), Float.toString(rcr));
    }

    /**
     * Get RCR and save to Redis Hash for all businesses having transactions during previous month
     *
     * @param transM_1 Dataset of transaction data for previous month
     */
    private static void saveAllBuRCR(Dataset<Row> transM_1) {
        // Get all distinct `buId` of previous month
        Dataset<Row> buIds = transM_1.select("buId").distinct();

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

    /**
     * Update AllPi and RecurPi using daily data
     *
     * This method should be triggered periodically (daily) by a scheduler to update AllPi and RecurPi in Redis for
     * current month.
     *
     * @param dailyTrans daily transaction data
     */
    public static void updateDailyPi(Dataset<Row> dailyTrans) {
        updateDailyAllPi(dailyTrans);
        updateDailyRecurPi(dailyTrans);
    }

    /**
     * Update AllPi and RecurPi using daily data
     *
     * This method should be triggered periodically (daily) by a scheduler to update AllPi and RecurPi in Redis for
     * current month.
     *
     * @param spark SparkSession
     */
    public static void updateDailyPi(SparkSession spark) {
        // Load transactions table
        Dataset<Row> transactions = spark.read()
                .format("org.apache.spark.sql.redis")
                .option("spark.redis.host", redisHost)
                .option("spark.redis.port", redisPort)
                .option("table", "Transactions")
                .option("key.column", "id")
                .load();

        // Get data for current day and group by `buId`
        Dataset<Row> dailyTrans = transactions.where(functions.year(functions.col("tDate")).equalTo(currentYear)
                .and(functions.month(functions.col("tDate")).equalTo(currentMonth))
                .and(functions.dayofmonth(functions.col("tDate")).equalTo(currentDay)));

        updateDailyPi(dailyTrans);
    }

    /**
     * Clear Lpi, AllPi, RecurPi keys of previous month and update them using new data
     *
     * This method should be called at the beginning of a new calendar month, when we need to sink recurrent customer
     * ratio of previous month into DB then clear and recalculate Lpi, AllPi, RecurPi.
     *
     * @param spark SparkSession
     */
    public static void updateMonthlyCache(SparkSession spark) {
        // Load transactions table
        Dataset<Row> transactions = spark.read()
                .format("org.apache.spark.sql.redis")
                .option("spark.redis.host", redisHost)
                .option("spark.redis.port", redisPort)
                .option("table", "Transactions")
                .option("key.column", "id")
                .load();

        // Keep past 12 months data
        transactions.createOrReplaceTempView("trans");
        String q = String.format("select * from trans where tDate >= '%04d-%02d-01' and tDate < '%04d-%02d-01'",
                currentYear - 1, currentMonth, currentYear, currentMonth);
        Dataset<Row> trans1Y = spark.sql(q);

        // Get data for previous month
        Dataset<Row> transM_1 = trans1Y.where(functions.year(functions.col("tDate")).equalTo(previousMonth_yyyy)
                .and(functions.month(functions.col("tDate")).equalTo(previousMonth_mm)));

        // Get data for current month
        Dataset<Row> transM = transactions.where(functions.year(functions.col("tDate")).equalTo(currentYear)
                .and(functions.month(functions.col("tDate")).equalTo(currentMonth)));

        saveAllBuRCR(transM_1);
        updateMonthlyLpi(trans1Y);
        updateMonthlyAllPi(transM);
        updateMonthlyRecurPi(transM);
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
