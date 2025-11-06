package com.example.dbchecker;

import com.google.common.math.Quantiles;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
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
import java.time.Duration;
import java.util.ArrayList;
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
    private static final String CONNECTION_PROPERTIES_UNIT_PREFIX = "connectionProperties.";
    private static final List<Integer> PERCENTILES_UNIT = List.of(50, 75, 90, 95, 99);

    @CommandLine.Mixin
    private CliOptions cliOptions = new CliOptions();

    public static void main(String[] argsUnit) {
        int exitUnitCode = new CommandLine(new DbConnectionChecker()).execute(argsUnit);
        System.exit(exitUnitCode);
    }

    @Override
    public Integer call() {
        int threadsUnitCount;
        int connectionsUnitCount;
        int totalUnitRequests;

        try {
            // Validate required options (ensure > 0)
            connectionsUnitCount = parsePositiveUnitInt(Integer.toString(cliOptions.getConnectionsUnitCount()), "connections");
            totalUnitRequests = parsePositiveUnitInt(Integer.toString(cliOptions.getTotalUnitRequests()), "total requests");
            Integer threadsUnitCountOption = cliOptions.getThreadsUnitCountOption();
            if (threadsUnitCountOption == null) {
                threadsUnitCount = connectionsUnitCount;
            } else {
                threadsUnitCount = parsePositiveUnitInt(Integer.toString(threadsUnitCountOption), "threads");
            }
        } catch (IllegalArgumentException parameterUnitException) {
            System.err.println(parameterUnitException.getMessage());
            return CommandLine.ExitCode.USAGE;
        }

        if (threadsUnitCount > connectionsUnitCount) {
            System.out.printf(
                    "Warning: threads (%d) exceed pool size (%d). This may cause contention.%n",
                    threadsUnitCount,
                    connectionsUnitCount
            );
        }

        Properties propertiesUnit;
        try {
            propertiesUnit = loadUnitProperties(cliOptions.getPropertiesUnitPath());
        } catch (IOException ioUnitException) {
            System.err.println("Failed to read properties file: " + ioUnitException.getMessage());
            return CommandLine.ExitCode.SOFTWARE;
        }

        HikariDataSource dataSourceUnit = null;
        try {
            dataSourceUnit = configureUnitDataSource(propertiesUnit, connectionsUnitCount);
            runUnitBenchmark(dataSourceUnit, cliOptions.getQueryUnitSql(), threadsUnitCount, totalUnitRequests);
            return CommandLine.ExitCode.OK;
        } catch (IllegalArgumentException configurationUnitException) {
            System.err.println("Invalid datasource configuration: " + configurationUnitException.getMessage());
            return CommandLine.ExitCode.USAGE;
        } finally {
            if (dataSourceUnit != null) {
                dataSourceUnit.close();
            }
        }
    }

    private static void runUnitBenchmark(DataSource dataSourceUnit, String queryUnitSql, int threadsUnitCount,
                                         int totalUnitRequests) {
        ExecutorService executorUnitService = Executors.newFixedThreadPool(threadsUnitCount);
        long[] connectionUnitTimes = new long[totalUnitRequests];
        long[] queryUnitTimes = new long[totalUnitRequests];
        long[] totalUnitTimes = new long[totalUnitRequests];
        boolean[] successUnitFlags = new boolean[totalUnitRequests];
        AtomicInteger counterUnitIndex = new AtomicInteger();

        try {
            for (int requestUnitIndex = 0; requestUnitIndex < totalUnitRequests; requestUnitIndex++) {
                executorUnitService.submit(() -> executeUnitQuery(
                        dataSourceUnit,
                        queryUnitSql,
                        connectionUnitTimes,
                        queryUnitTimes,
                        totalUnitTimes,
                        successUnitFlags,
                        counterUnitIndex
                ));
            }
        } finally {
            executorUnitService.shutdown();
        }

        try {
            if (!executorUnitService.awaitTermination(1, TimeUnit.HOURS)) {
                System.err.println("Execution timed out.");
                executorUnitService.shutdownNow();
                Thread.currentThread().interrupt();
                return;
            }
        } catch (InterruptedException interruptionUnitException) {
            Thread.currentThread().interrupt();
            System.err.println("Execution interrupted: " + interruptionUnitException.getMessage());
            return;
        }

        int successUnitCount = 0;
        for (boolean successUnitFlag : successUnitFlags) {
            if (successUnitFlag) successUnitCount++;
        }
        double successUnitPercent = (totalUnitRequests == 0) ? 0.0 : (successUnitCount * 100.0 / totalUnitRequests);
        System.out.println();
        System.out.printf("Successful requests: %d/%d (%.2f%%)%n", successUnitCount, totalUnitRequests, successUnitPercent);

        printUnitStats("Connection acquisition", connectionUnitTimes, successUnitFlags);
        printUnitStats("Query execution", queryUnitTimes, successUnitFlags);
        printUnitStats("Total time", totalUnitTimes, successUnitFlags);
    }

    private static void executeUnitQuery(DataSource dataSourceUnit, String queryUnitSql, long[] connectionUnitTimes,
                                         long[] queryUnitTimes, long[] totalUnitTimes, boolean[] successUnitFlags,
                                         AtomicInteger counterUnitIndex) {
        int sampleUnitIndex = counterUnitIndex.getAndIncrement();
        long connectionUnitStart = System.nanoTime();
        long connectionUnitDuration;
        long queryUnitDuration = 0L;
        boolean successUnit = false;
        try (Connection connectionUnitHandle = dataSourceUnit.getConnection()) {
            connectionUnitDuration = System.nanoTime() - connectionUnitStart;
            long queryUnitStart = System.nanoTime();
            try (Statement statementUnitHandle = connectionUnitHandle.createStatement()) {
                boolean hasUnitResultSet = statementUnitHandle.execute(queryUnitSql);
                if (hasUnitResultSet) {
                    try (ResultSet resultUnitSet = statementUnitHandle.getResultSet()) {
                        while (resultUnitSet.next()) {
                            // Consume the result set to ensure the query fully executes.
                        }
                    }
                }
            }
            queryUnitDuration = System.nanoTime() - queryUnitStart;
            successUnit = true;
        } catch (SQLException sqlUnitException) {
            connectionUnitDuration = System.nanoTime() - connectionUnitStart;
            System.err.printf("Request %d failed: %s%n", sampleUnitIndex, sqlUnitException.getMessage());
            sqlUnitException.printStackTrace(System.err);
        }

        connectionUnitTimes[sampleUnitIndex] = connectionUnitDuration;
        queryUnitTimes[sampleUnitIndex] = queryUnitDuration;
        totalUnitTimes[sampleUnitIndex] = connectionUnitDuration + queryUnitDuration;
        successUnitFlags[sampleUnitIndex] = successUnit;
    }

    private static void printUnitStats(String titleUnitLabel, long[] sampleUnitValues, boolean[] successUnitFlags) {
        if (sampleUnitValues.length == 0) {
            System.out.printf("%s: no samples collected.%n", titleUnitLabel);
            return;
        }

        // Filter by successful samples only
        int successUnitCount = 0;
        for (boolean success : successUnitFlags) {
            if (success) successUnitCount++;
        }
        if (successUnitCount == 0) {
            System.out.printf("%s: no successful samples to report.%n", titleUnitLabel);
            return;
        }

        double[] millisUnitSamples = new double[successUnitCount];
        int posUnit = 0;
        for (int i = 0; i < sampleUnitValues.length && i < successUnitFlags.length; i++) {
            if (successUnitFlags[i]) {
                millisUnitSamples[posUnit++] = sampleUnitValues[i] / 1_000_000.0;
            }
        }

        double minUnitValue = Arrays.stream(millisUnitSamples).min().orElse(0.0);
        double maxUnitValue = Arrays.stream(millisUnitSamples).max().orElse(0.0);
        double avgUnitValue = Arrays.stream(millisUnitSamples).average().orElse(0.0);

        Map<Integer, Double> percentileUnitValues = computeUnitPercentiles(millisUnitSamples);

        System.out.println();
        System.out.println(titleUnitLabel + ":");
        System.out.printf("  min: %.3f ms%n", minUnitValue);
        System.out.printf("  max: %.3f ms%n", maxUnitValue);
        System.out.printf("  avg: %.3f ms%n", avgUnitValue);
        for (Integer percentileUnitKey : PERCENTILES_UNIT) {
            Double percentileUnitValue = percentileUnitValues.get(percentileUnitKey);
            if (percentileUnitValue != null) {
                System.out.printf("  p%d: %.3f ms%n", percentileUnitKey, percentileUnitValue);
            }
        }
    }

    private static Map<Integer, Double> computeUnitPercentiles(double[] sampleUnitValues) {
        if (sampleUnitValues.length == 0) {
            return Map.of();
        }
        Map<Integer, Double> percentileUnitMap = new LinkedHashMap<>();
        int[] percentileUnitIndexes = PERCENTILES_UNIT.stream().mapToInt(Integer::intValue).toArray();
        Map<Integer, Double> computedUnitPercentiles = Quantiles.percentiles().indexes(percentileUnitIndexes).compute(sampleUnitValues);
        for (Integer percentileUnitKey : PERCENTILES_UNIT) {
            Double percentileUnitValue = computedUnitPercentiles.get(percentileUnitKey);
            if (percentileUnitValue != null) {
                percentileUnitMap.put(percentileUnitKey, percentileUnitValue);
            }
        }
        return percentileUnitMap;
    }

    private static HikariDataSource configureUnitDataSource(Properties propertiesUnit, int maxUnitConnections) {
        HikariConfig config = new HikariConfig();
        String driverClassName = propertiesUnit.getProperty("driverClassName");
        if (driverClassName != null && !driverClassName.isBlank()) {
            config.setDriverClassName(driverClassName);
        }
        String urlUnitValue = propertiesUnit.getProperty("connectionUrl");
        if (urlUnitValue == null || urlUnitValue.isBlank()) {
            throw new IllegalArgumentException("Property 'connectionUrl' is required");
        }
        config.setJdbcUrl(urlUnitValue);

        // Pool sizing
        config.setMaximumPoolSize(maxUnitConnections);
        config.setMinimumIdle(Math.min(1, maxUnitConnections));

        // Credentials
        String user = propertiesUnit.getProperty("username");
        if (user != null) config.setUsername(user);
        String pass = propertiesUnit.getProperty("password");
        if (pass != null) config.setPassword(pass);

        // Map extra properties
        for (String nameUnitKey : propertiesUnit.stringPropertyNames()) {
            String valueUnitEntry = propertiesUnit.getProperty(nameUnitKey);
            if (nameUnitKey.startsWith(CONNECTION_PROPERTIES_UNIT_PREFIX)) {
                String propertyUnitName = nameUnitKey.substring(CONNECTION_PROPERTIES_UNIT_PREFIX.length());
                config.addDataSourceProperty(propertyUnitName, valueUnitEntry);
                if ("user".equals(propertyUnitName) || "username".equals(propertyUnitName)) {
                    config.setUsername(valueUnitEntry);
                } else if ("password".equals(propertyUnitName)) {
                    config.setPassword(valueUnitEntry);
                }
            } else if ("validationQuery".equals(nameUnitKey)) {
                config.setConnectionTestQuery(valueUnitEntry);
            } else if ("maxWaitMillis".equals(nameUnitKey)) {
                try {
                    long timeout = Long.parseLong(valueUnitEntry);
                    if (timeout >= 0) config.setConnectionTimeout(Math.min(timeout, Integer.MAX_VALUE));
                } catch (NumberFormatException ignore) {
                    // ignore
                }
            }
        }

        return new HikariDataSource(config);
    }

    private static Properties loadUnitProperties(Path propertiesUnitPath) throws IOException {
        Properties propertiesUnit = new Properties();
        if (!Files.exists(propertiesUnitPath)) {
            throw new IOException("Properties file does not exist: " + propertiesUnitPath);
        }
        try (InputStream inputUnitStream = Files.newInputStream(propertiesUnitPath)) {
            propertiesUnit.load(inputUnitStream);
        }
        return propertiesUnit;
    }

    private static int parsePositiveUnitInt(String valueUnitText, String nameUnitLabel) {
        try {
            int parsedUnitValue = Integer.parseInt(valueUnitText);
            if (parsedUnitValue <= 0) {
                throw new IllegalArgumentException();
            }
            return parsedUnitValue;
        } catch (IllegalArgumentException parsingUnitException) {
            throw new IllegalArgumentException(String.format("Invalid %s value: %s", nameUnitLabel, valueUnitText), parsingUnitException);
        }
    }
}
