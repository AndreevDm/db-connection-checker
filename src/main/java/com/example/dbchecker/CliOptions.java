package com.example.dbchecker;

import picocli.CommandLine;

import java.nio.file.Path;

/**
 * Encapsulates CLI parameters for DbConnectionChecker.
 */
public class CliOptions {
    public enum PoolType { HIKARI, DBCP }

    @CommandLine.Option(names = {"-p", "--properties"}, required = true, paramLabel = "<propertiesFile>",
            description = "Path to the database properties file.")
    private Path propertiesPath;

    @CommandLine.Option(names = {"-q", "--query"}, required = true, paramLabel = "<query>",
            description = "SQL query to execute for benchmarking.")
    private String querySql;

    @CommandLine.Option(names = {"-c", "--connections"}, required = true, paramLabel = "<connections>",
            description = "Number of connections in the pool.")
    private int connectionsCount;

    @CommandLine.Option(names = {"-t", "--threads"}, paramLabel = "<threads>",
            description = "Optional number of threads. If omitted, defaults to the number of connections.")
    private Integer threadsCountOption;

    @CommandLine.Option(names = {"-n", "--total-requests"}, required = true, paramLabel = "<totalRequests>",
            description = "Total number of requests to execute across all threads.")
    private int totalRequests;

    @CommandLine.Option(names = {"--pool"}, defaultValue = "HIKARI",
            description = "Connection pool implementation to use: ${COMPLETION-CANDIDATES} (default: ${DEFAULT-VALUE}).")
    private PoolType poolType;

    public Path getPropertiesPath() {
        return propertiesPath;
    }

    public String getQuerySql() {
        return querySql;
    }

    public int getConnectionsCount() {
        return connectionsCount;
    }

    public Integer getThreadsCountOption() {
        return threadsCountOption;
    }

    public int getTotalRequests() {
        return totalRequests;
    }

    public PoolType getPoolType() {
        return poolType;
    }
}
