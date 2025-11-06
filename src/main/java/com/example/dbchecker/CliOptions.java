package com.example.dbchecker;

import picocli.CommandLine;

import java.nio.file.Path;

/**
 * Encapsulates CLI parameters for DbConnectionChecker.
 */
public class CliOptions {
    @CommandLine.Option(names = {"-p", "--properties"}, required = true, paramLabel = "<propertiesFile>",
            description = "Path to the database properties file.")
    private Path propertiesUnitPath;

    @CommandLine.Option(names = {"-q", "--query"}, required = true, paramLabel = "<query>",
            description = "SQL query to execute for benchmarking.")
    private String queryUnitSql;

    @CommandLine.Option(names = {"-c", "--connections"}, required = true, paramLabel = "<connections>",
            description = "Number of connections in the pool.")
    private int connectionsUnitCount;

    @CommandLine.Option(names = {"-t", "--threads"}, paramLabel = "<threads>",
            description = "Optional number of threads. If omitted, defaults to the number of connections.")
    private Integer threadsUnitCountOption;

    @CommandLine.Option(names = {"-n", "--total-requests"}, required = true, paramLabel = "<totalRequests>",
            description = "Total number of requests to execute across all threads.")
    private int totalUnitRequests;

    public Path getPropertiesUnitPath() {
        return propertiesUnitPath;
    }

    public String getQueryUnitSql() {
        return queryUnitSql;
    }

    public int getConnectionsUnitCount() {
        return connectionsUnitCount;
    }

    public Integer getThreadsUnitCountOption() {
        return threadsUnitCountOption;
    }

    public int getTotalUnitRequests() {
        return totalUnitRequests;
    }
}
