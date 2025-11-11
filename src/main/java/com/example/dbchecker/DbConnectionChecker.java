package com.example.dbchecker;

import com.google.common.math.Quantiles;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.apache.commons.dbcp2.BasicDataSource;
import picocli.CommandLine;

import javax.sql.DataSource;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

@CommandLine.Command(
        name = "db-connection-checker",
        mixinStandardHelpOptions = true,
        description = "Benchmarks database connection acquisition and query execution times."
)
public class DbConnectionChecker implements Callable<Integer> {
    private static final String CONNECTION_PROPERTIES_PREFIX = "connectionProperties.";
    private static final List<Integer> PERCENTILES = List.of(50, 75, 90, 95, 99);

    @CommandLine.Mixin
    private CliOptions cliOptions = new CliOptions();

    public static void main(String[] args) {
        int exitCode = new CommandLine(new DbConnectionChecker()).execute(args);
        System.exit(exitCode);
    }

    @Override
    public Integer call() {
        int threadsCount;
        int connectionsCount;
        int totalRequests;

        try {
            // Validate required options (ensure > 0)
            connectionsCount = parsePositiveInt(Integer.toString(cliOptions.getConnectionsCount()), "connections");
            int requestsPerThread = parsePositiveInt(Integer.toString(cliOptions.getTotalRequests()), "requests per thread");
            Integer threadsCountOption = cliOptions.getThreadsCountOption();
            if (threadsCountOption == null) {
                threadsCount = connectionsCount;
            } else {
                threadsCount = parsePositiveInt(Integer.toString(threadsCountOption), "threads");
            }
            totalRequests = threadsCount * requestsPerThread;
        } catch (IllegalArgumentException parameterException) {
            System.err.println(parameterException.getMessage());
            return CommandLine.ExitCode.USAGE;
        }

        if (threadsCount > connectionsCount) {
            System.out.printf(
                    "Warning: threads (%d) exceed pool size (%d). This may cause contention.%n",
                    threadsCount,
                    connectionsCount
            );
        }

        Properties properties;
        try {
            properties = loadProperties(cliOptions.getPropertiesPath());
        } catch (IOException ioException) {
            System.err.println("Failed to read properties file: " + ioException.getMessage());
            return CommandLine.ExitCode.SOFTWARE;
        }

        DataSource dataSource = null;
        try {
            dataSource = configureDataSource(properties, connectionsCount, cliOptions.getPoolType());
            runBenchmark(dataSource, cliOptions.getQuerySql(), threadsCount, totalRequests);
            return CommandLine.ExitCode.OK;
        } catch (IllegalArgumentException configurationException) {
            System.err.println("Invalid datasource configuration: " + configurationException.getMessage());
            return CommandLine.ExitCode.USAGE;
        } finally {
            if (dataSource instanceof AutoCloseable closeable) {
                try {
                    closeable.close();
                } catch (Exception closeException) {
                    System.err.println("Failed to close DataSource: " + closeException.getMessage());
                }
            }
        }
    }

    private static void runBenchmark(DataSource dataSource, String querySql, int threadsCount,
                                     int totalRequests) {
        ExecutorService executorService = Executors.newFixedThreadPool(threadsCount);
        long[] connectionTimesNs = new long[totalRequests];
        long[] queryTimesNs = new long[totalRequests];
        long[] totalTimesNs = new long[totalRequests];
        boolean[] successFlags = new boolean[totalRequests];
        AtomicInteger counterIndex = new AtomicInteger();
        // Progress tracking: report every 10%
        AtomicInteger completedCount = new AtomicInteger();
        AtomicInteger nextProgressPercent = new AtomicInteger(10);

        try {
            for (int requestIndex = 0; requestIndex < totalRequests; requestIndex++) {
                executorService.submit(() -> executeQuery(
                        dataSource,
                        querySql,
                        connectionTimesNs,
                        queryTimesNs,
                        totalTimesNs,
                        successFlags,
                        counterIndex,
                        completedCount,
                        totalRequests,
                        nextProgressPercent
                ));
            }
        } finally {
            executorService.shutdown();
        }

        try {
            if (!executorService.awaitTermination(1, TimeUnit.HOURS)) {
                System.err.println("Execution timed out.");
                executorService.shutdownNow();
                Thread.currentThread().interrupt();
                return;
            }
        } catch (InterruptedException interruptionException) {
            Thread.currentThread().interrupt();
            System.err.println("Execution interrupted: " + interruptionException.getMessage());
            return;
        }

        int successCount = 0;
        for (boolean successFlag : successFlags) {
            if (successFlag) successCount++;
        }
        double successPercent = (totalRequests == 0) ? 0.0 : (successCount * 100.0 / totalRequests);
        System.out.println();
        System.out.printf("Successful requests: %d/%d (%.2f%%)%n", successCount, totalRequests, successPercent);

        printStats("Connection acquisition", connectionTimesNs, successFlags);
        printStats("Query execution", queryTimesNs, successFlags);
        printStats("Total time", totalTimesNs, successFlags);
    }

    private static void executeQuery(DataSource dataSource, String querySql, long[] connectionTimesNs,
                                     long[] queryTimesNs, long[] totalTimesNs, boolean[] successFlags,
                                     AtomicInteger counterIndex,
                                     AtomicInteger completedCount,
                                     int totalRequests,
                                     AtomicInteger nextProgressPercent) {
        int sampleIndex = counterIndex.getAndIncrement();
        long connectionStartNs = System.nanoTime();
        long connectionDurationNs;
        long queryDurationNs = 0L;
        boolean success = false;
        try (Connection connection = dataSource.getConnection()) {
            connectionDurationNs = System.nanoTime() - connectionStartNs;
            long queryStartNs = System.nanoTime();
            try (Statement statement = connection.createStatement()) {
                boolean hasResultSet = statement.execute(querySql);
                if (hasResultSet) {
                    try (ResultSet resultSet = statement.getResultSet()) {
                        while (resultSet.next()) {
                            // Consume the result set to ensure the query fully executes.
                        }
                    }
                }
            }
            queryDurationNs = System.nanoTime() - queryStartNs;
            success = true;
        } catch (SQLException sqlException) {
            connectionDurationNs = System.nanoTime() - connectionStartNs;
            System.err.printf("Request %d failed: %s%n", sampleIndex, sqlException.getMessage());
            sqlException.printStackTrace(System.err);
        }

        connectionTimesNs[sampleIndex] = connectionDurationNs;
        queryTimesNs[sampleIndex] = queryDurationNs;
        totalTimesNs[sampleIndex] = connectionDurationNs + queryDurationNs;
        successFlags[sampleIndex] = success;

        // Progress update
        int done = completedCount.incrementAndGet();
        if (totalRequests > 0) {
            int percent = (int) Math.floor(done * 100.0 / totalRequests);
            while (true) {
                int target = nextProgressPercent.get();
                if (percent >= target && target <= 100) {
                    if (nextProgressPercent.compareAndSet(target, target + 10)) {
                        System.out.printf("Progress: %d%%%n", target);
                        continue;
                    }
                } else {
                    break;
                }
            }
        }
    }

    private static void printStats(String title, long[] sampleTimesNs, boolean[] successFlags) {
        if (sampleTimesNs.length == 0) {
            System.out.printf("%s: no samples collected.%n", title);
            return;
        }

        // Filter by successful samples only
        int successCount = 0;
        for (boolean success : successFlags) {
            if (success) successCount++;
        }
        if (successCount == 0) {
            System.out.printf("%s: no successful samples to report.%n", title);
            return;
        }

        double[] samplesMs = new double[successCount];
        int pos = 0;
        for (int i = 0; i < sampleTimesNs.length && i < successFlags.length; i++) {
            if (successFlags[i]) {
                samplesMs[pos++] = sampleTimesNs[i] / 1_000_000.0;
            }
        }

        double minMs = Arrays.stream(samplesMs).min().orElse(0.0);
        double maxMs = Arrays.stream(samplesMs).max().orElse(0.0);
        double avgMs = Arrays.stream(samplesMs).average().orElse(0.0);

        Map<Integer, Double> percentileValues = computePercentiles(samplesMs);

        System.out.println();
        System.out.println(title + ":");
        System.out.printf("  min: %.3f ms%n", minMs);
        System.out.printf("  max: %.3f ms%n", maxMs);
        System.out.printf("  avg: %.3f ms%n", avgMs);
        for (Integer percentile : PERCENTILES) {
            Double percentileValue = percentileValues.get(percentile);
            if (percentileValue != null) {
                System.out.printf("  p%d: %.3f ms%n", percentile, percentileValue);
            }
        }
    }

    private static Map<Integer, Double> computePercentiles(double[] samplesMs) {
        if (samplesMs.length == 0) {
            return Map.of();
        }
        Map<Integer, Double> percentileMap = new LinkedHashMap<>();
        int[] percentileIndexes = PERCENTILES.stream().mapToInt(Integer::intValue).toArray();
        Map<Integer, Double> computedPercentiles = Quantiles.percentiles().indexes(percentileIndexes).compute(samplesMs);
        for (Integer percentile : PERCENTILES) {
            Double percentileValue = computedPercentiles.get(percentile);
            if (percentileValue != null) {
                percentileMap.put(percentile, percentileValue);
            }
        }
        return percentileMap;
    }

    private static DataSource configureDataSource(Properties properties, int maxConnections, CliOptions.PoolType poolType) {
        if (poolType == null) poolType = CliOptions.PoolType.HIKARI;
        return switch (poolType) {
            case HIKARI -> buildHikari(properties, maxConnections);
            case DBCP -> buildDbcp(properties, maxConnections);
        };
    }

    private static HikariDataSource buildHikari(Properties properties, int maxConnections) {
        HikariConfig config = new HikariConfig();
        String driverClassName = properties.getProperty("driverClassName");
        if (driverClassName != null && !driverClassName.isBlank()) {
            config.setDriverClassName(driverClassName);
        }
        String url = properties.getProperty("connectionUrl");
        if (url == null || url.isBlank()) {
            throw new IllegalArgumentException("Property 'connectionUrl' is required");
        }
        config.setJdbcUrl(url);

        // Pool sizing
        config.setMaximumPoolSize(maxConnections);
        config.setMinimumIdle(maxConnections);

        // Credentials
        String user = properties.getProperty("username");
        if (user != null) config.setUsername(user);
        String pass = properties.getProperty("password");
        if (pass != null) config.setPassword(pass);

        // Map extra properties
        for (String nameKey : properties.stringPropertyNames()) {
            String value = properties.getProperty(nameKey);
            if (nameKey.startsWith(CONNECTION_PROPERTIES_PREFIX)) {
                String propertyName = nameKey.substring(CONNECTION_PROPERTIES_PREFIX.length());
                config.addDataSourceProperty(propertyName, value);
                if ("user".equals(propertyName) || "username".equals(propertyName)) {
                    config.setUsername(value);
                } else if ("password".equals(propertyName)) {
                    config.setPassword(value);
                }
            } else if ("validationQuery".equals(nameKey)) {
                config.setConnectionTestQuery(value);
            } else if ("maxWaitMillis".equals(nameKey)) {
                try {
                    long timeout = Long.parseLong(value);
                    if (timeout >= 0) config.setConnectionTimeout(Math.min(timeout, Integer.MAX_VALUE));
                } catch (NumberFormatException ignore) {
                    // ignore
                }
            }
        }

        return new HikariDataSource(config);
    }

    private static BasicDataSource buildDbcp(Properties properties, int maxConnections) {
        BasicDataSource ds = new BasicDataSource();
        String driverClassName = properties.getProperty("driverClassName");
        if (driverClassName != null && !driverClassName.isBlank()) {
            ds.setDriverClassName(driverClassName);
        }
        String url = properties.getProperty("connectionUrl");
        if (url == null || url.isBlank()) {
            throw new IllegalArgumentException("Property 'connectionUrl' is required");
        }
        ds.setUrl(url);

        // Pool sizing equivalent
        ds.setMaxTotal(maxConnections);
        ds.setMaxIdle(maxConnections);
        ds.setMinIdle(maxConnections);

        // Credentials
        String user = properties.getProperty("username");
        if (user != null) ds.setUsername(user);
        String pass = properties.getProperty("password");
        if (pass != null) ds.setPassword(pass);

        // Map extra properties
        for (String nameKey : properties.stringPropertyNames()) {
            String value = properties.getProperty(nameKey);
            if (nameKey.startsWith(CONNECTION_PROPERTIES_PREFIX)) {
                String propertyName = nameKey.substring(CONNECTION_PROPERTIES_PREFIX.length());
                ds.addConnectionProperty(propertyName, value);
                if ("user".equals(propertyName) || "username".equals(propertyName)) {
                    ds.setUsername(value);
                } else if ("password".equals(propertyName)) {
                    ds.setPassword(value);
                }
            } else if ("validationQuery".equals(nameKey)) {
                ds.setValidationQuery(value);
                ds.setTestOnBorrow(true);
            } else if ("maxWaitMillis".equals(nameKey)) {
                try {
                    long timeout = Long.parseLong(value);
                    if (timeout >= 0) ds.setMaxWait(java.time.Duration.ofMillis(timeout));
                } catch (NumberFormatException ignore) {
                    // ignore
                }
            }
        }
        return ds;
    }

    private static Properties loadProperties(Path propertiesPath) throws IOException {
        Properties properties = new Properties();
        if (!Files.exists(propertiesPath)) {
            throw new IOException("Properties file does not exist: " + propertiesPath);
        }
        try (InputStream inputStream = Files.newInputStream(propertiesPath)) {
            properties.load(inputStream);
        }
        return properties;
    }

    private static int parsePositiveInt(String valueText, String nameLabel) {
        try {
            int parsedValue = Integer.parseInt(valueText);
            if (parsedValue <= 0) {
                throw new IllegalArgumentException();
            }
            return parsedValue;
        } catch (IllegalArgumentException parsingException) {
            throw new IllegalArgumentException(String.format("Invalid %s value: %s", nameLabel, valueText), parsingException);
        }
    }
}
