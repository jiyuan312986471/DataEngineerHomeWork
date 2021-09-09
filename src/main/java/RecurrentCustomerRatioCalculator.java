import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import redis.clients.jedis.Jedis;

import java.time.LocalDate;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class RecurrentCustomerRatioCalculator {
    private final static String redisHost = "localhost";
    private final static int redisPort = 6379;

    private final static int currentYear = LocalDate.now().getYear();
    private final static int currentMonth = LocalDate.now().getMonthValue() - 1;

    public static long getRecurPiCount(String buId) {
        Jedis jedis = new Jedis(redisHost, redisPort);
        return jedis.scard("RecurPi:" + buId);
    }

    public static long getAllPiCount(String buId) {
        Jedis jedis = new Jedis(redisHost, redisPort);
        return jedis.scard("AllPi:" + buId);
    }

    public static float calculateRCR(String buId) {
        Jedis jedis = new Jedis(redisHost, redisPort);

        // Check AllPi count first, no need to go further if 0 returned
        float cntAllPi = jedis.scard("AllPi:" + buId);
        if (cntAllPi == 0) {
            return 0;
        }

        float cntRecurPi = jedis.scard("RecurPi:" + buId);
        return cntRecurPi / cntAllPi;
    }

    public static Map<String, String> historicalRCR(String buId) {
        Jedis jedis = new Jedis(redisHost, redisPort);

        // History
        Map<String, String> history = jedis.hgetAll("Rcr:" + buId);

        // Current month
        // Check AllPi count first, no need to go further if 0 returned
        float cntAllPi = jedis.scard("AllPi:" + buId);
        if (cntAllPi == 0) {
            return history;
        }
        float cntRecurPi = jedis.scard("RecurPi:" + buId);
        float rcr = cntRecurPi / cntAllPi;

        // Merge history and current
        history.put(String.format("%d-%d", currentYear, currentMonth), Float.toString(rcr));

        return history;
    }

    public static void main(String[] args) {
        SparkSession spark = SparkSession
                .builder()
                .master("local[*]")
                .appName("RecurrentCustomerRatioCalculate")
                .getOrCreate();

        // Load transactions table
        Dataset<Row> transactions = spark.read()
                .format("org.apache.spark.sql.redis")
                .option("spark.redis.host", redisHost)
                .option("spark.redis.port", redisPort)
                .option("table", "Transactions")
                .option("key.column", "id")
                .load();

        // Get distinct buId for currentMonth
        Dataset<Row> allBuId = transactions
                .where(functions.year(functions.col("tDate")).equalTo(currentYear)
                    .and(functions.month(functions.col("tDate")).equalTo(currentMonth)))
                .select("buId")
                .distinct();

        // Select 5 business to display Recurrent Customer Ratio
        List<Row> buIds = allBuId.takeAsList(5);
        for (Row r: buIds) {
            String buId = r.getString(0);
            float rcr = calculateRCR(buId);
            System.out.printf("Current Month's Recurrent Customer Ratio for Business{%s}: %.4f%n", buId, rcr);
        }
    }
}
