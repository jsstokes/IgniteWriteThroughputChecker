package org.example;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.query.SqlFieldsQuery;
import org.apache.ignite.IgniteCompute;
import org.apache.ignite.lang.IgniteCallable;
import org.apache.ignite.resources.IgniteInstanceResource;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.CountDownLatch;
import java.util.Random;
import java.util.UUID;
import org.apache.ignite.configuration.CacheConfiguration;
import java.util.List;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.io.Serializable;
import java.util.ArrayList;
/**
 * Apache Ignite Performance Testing Utility
 *
 * Usage: java IgniteReadWriteThroughputChecker <configFile> [mode1] [mode2] ...
 *
 * Test Modes:
 * - SELECT_ONLY: Read performance
 * - UPDATE_ONLY: Write performance
 * - CONCURRENT: Simultaneous reads/writes
 * - POPULATE: Create and populate test table
 * - REPEATED_AGGREGATE_QUERY: Aggregate query performance
 * - REPEATED_MULTI_AGGREGATE_QUERY: Multiple aggregate queries
 * - COMPUTE_JOB: Distributed compute jobs
 *
 * Example:
 * java IgniteReadWriteThroughputChecker /path/to/ignite-config.xml POPULATE  REPEATED_AGGREGATE_QUERY
 *
 * Default Settings:
 * - Initial Records: 10,000
 * -  Records per Aggregate: 5
 * - Query Repetitions: 20
 * - Batch Size: 100
 */
public class IgniteReadWriteThroughputChecker {
    public enum TestMode {
        SELECT_ONLY,
        UPDATE_ONLY,
        CONCURRENT,
        POPULATE,
        REPEATED_AGGREGATE_QUERY,
        REPEATED_MULTI_AGGREGATE_QUERY,
        COMPUTE_JOB  // Standalone compute job mode
    }

    private final int testDurationSeconds;
    private final String tableName;
    private final int initialRecordCount;
    private Ignite ignite;
    private IgniteCache<?, ?> cache;

    private static final int RECORDS_PER_AGGREGATE = 5;
    private static final int DEFAULT_QUERY_REPETITIONS = 20;

    public IgniteReadWriteThroughputChecker(String tableName, int testDurationSeconds, int initialRecordCount) {
        this.testDurationSeconds = testDurationSeconds;
        this.tableName = tableName;
        this.initialRecordCount = initialRecordCount;
    }

    public void initialize(String configFile) {
        this.ignite = initializeIgniteClient(configFile);
        this.cache = initializeCache();
    }

    public void createAndPopulateTable() {
        createTableIfNotExists(cache);
        populateInitialRecords(cache);
    }

    private Ignite initializeIgniteClient(String configFile) {
        Ignition.setClientMode(true);

        return Ignition.start(configFile);
    }

    private IgniteCache<?, ?> initializeCache() {
        CacheConfiguration config = new CacheConfiguration("TEST_CACHE1_" + tableName.toUpperCase());
        config.setStatisticsEnabled(true);
        return ignite.getOrCreateCache(config);
    }

    private void createTableIfNotExists(IgniteCache<?, ?> cache) {
        // Drop existing table first
        String dropTableSql = String.format("DROP TABLE IF EXISTS %s", tableName);
        cache.query(new SqlFieldsQuery(dropTableSql)).getAll();

        String createTableSql = String.format(
                "CREATE TABLE IF NOT EXISTS %s (" +
                        "id UUID, " +
                        "aggregate_id UUID, " +
                        "group_id UUID, " +
                        "persisted_at TIMESTAMP, " +
                        "created_at TIMESTAMP, " +
                        "payload VARCHAR, " +
                        "PRIMARY KEY (id, aggregate_id, group_id, persisted_at))", tableName);

        cache.query(new SqlFieldsQuery(createTableSql)).getAll();

        String createIndexSql = String.format(
                "CREATE INDEX IF NOT EXISTS idx_%s_aggregate_id ON %s (aggregate_id)",
                tableName.toLowerCase(), tableName);

        cache.query(new SqlFieldsQuery(createIndexSql)).getAll();
    }

    private void populateInitialRecords(IgniteCache cache) {
        System.out.println("Populating initial " + initialRecordCount + " records...");
        Random random = new Random();

        // Calculate how many aggregate_ids we need
        int numberOfAggregates = (int) Math.ceil((double) initialRecordCount / RECORDS_PER_AGGREGATE);

        for (int i = 0; i < numberOfAggregates; i++) {
            UUID aggregateId = UUID.randomUUID();

            // Create RECORDS_PER_AGGREGATE records for each aggregate_id
            for (int j = 0; j < RECORDS_PER_AGGREGATE; j++) {
                SqlFieldsQuery query = new SqlFieldsQuery(
                        String.format("INSERT INTO %s (id, aggregate_id, group_id, persisted_at, created_at, payload) " +
                                "VALUES (?, ?, ?, ?, ?, ?)", tableName))
                        .setArgs(
                                UUID.randomUUID(),        // unique id
                                aggregateId,              // shared aggregate_id
                                null,                     // group_id is null
                                new java.sql.Timestamp(System.currentTimeMillis()), // persisted_at
                                new java.sql.Timestamp(System.currentTimeMillis() - random.nextInt(86400000)), // created_at
                                "Sample payload " + UUID.randomUUID().toString()  // random payload
                        );
                cache.query(query).getAll();
            }
        }
        System.out.println("Initial population completed.");
    }

    private void runSelectThread(AtomicInteger selectCount, CountDownLatch latch) {
        Thread selectThread = new Thread(() -> {
            long startTime = System.currentTimeMillis();

            while (System.currentTimeMillis() - startTime < testDurationSeconds * 1000) {
                try {
                    SqlFieldsQuery query = new SqlFieldsQuery(
                            String.format("SELECT * FROM %s WHERE id > ? LIMIT 100", tableName))
                            .setArgs(new Random().nextInt(1000));

                    cache.query(query).getAll();
                    selectCount.incrementAndGet();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
            latch.countDown();
        });
        selectThread.start();
    }

    private void runUpdateThread(AtomicInteger updateCount, CountDownLatch latch) {
        Thread updateThread = new Thread(() -> {
            long startTime = System.currentTimeMillis();
            Random random = new Random();

            while (System.currentTimeMillis() - startTime < testDurationSeconds * 1000) {
                try {
                    SqlFieldsQuery query = new SqlFieldsQuery(
                            String.format("UPDATE %s SET value = ? WHERE id IN (SELECT id FROM %s WHERE id > ? LIMIT 1)",
                                    tableName, tableName))
                            .setArgs(
                                    random.nextDouble() * 1000,
                                    UUID.randomUUID()
                            );

                    cache.query(query).getAll();
                    updateCount.incrementAndGet();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
            latch.countDown();
        });
        updateThread.start();
    }

    private void runRepeatedAggregateQuery(int repetitions) {
        // First, get a random aggregate_id from the table
        SqlFieldsQuery findAggregateQuery = new SqlFieldsQuery(
                String.format("SELECT DISTINCT aggregate_id FROM %s LIMIT 1", tableName));

        List<List<?>> result = cache.query(findAggregateQuery).getAll();
        if (result.isEmpty() || result.get(0).isEmpty()) {
            System.out.println("No aggregate IDs found in table!");
            return;
        }

        UUID aggregateId = (UUID) result.get(0).get(0);
        System.out.println("Selected aggregate_id: " + aggregateId);

        // Prepare the query we'll repeat
        SqlFieldsQuery query = new SqlFieldsQuery(
                String.format("SELECT * FROM %s WHERE aggregate_id = ?", tableName))
                .setArgs(aggregateId);

        System.out.println("Executing query " + repetitions + " times...");

        // Execute and measure each repetition
        for (int i = 0; i < repetitions; i++) {
            long startTime = System.nanoTime();
            cache.query(query).getAll();
            long endTime = System.nanoTime();

            double milliseconds = (endTime - startTime) / 1_000_000.0;
            System.out.printf("Execution %d: %.2f ms%n", i + 1, milliseconds);
        }
    }

    private void runRepeatedMultiAggregateQuery(int repetitions) {
        SqlFieldsQuery findAggregatesQuery = new SqlFieldsQuery(
                String.format("SELECT DISTINCT aggregate_id FROM %s", tableName));

        List<List<?>> results = cache.query(findAggregatesQuery).getAll();
        if (results.isEmpty()) {
            System.out.println("No aggregate IDs found in table!");
            return;
        }

        System.out.printf("Found %d aggregate IDs. Will execute %d queries, switching aggregate_id each time...%n",
                results.size(), repetitions);

        executeWithDirectQueries(results, repetitions);
    }

    private void executeWithDirectQueries(List<List<?>> results, int repetitions) {
        double totalMs = 0;
        Random random = new Random();

        for (int i = 0; i < repetitions; i++) {
            UUID aggregateId = (UUID) results.get(random.nextInt(results.size())).get(0);

            long startTime = System.nanoTime();
            SqlFieldsQuery query = new SqlFieldsQuery(
                    String.format("SELECT * FROM %s WHERE aggregate_id = ?", tableName))
                    .setArgs(aggregateId);

            List<List<?>> queryResults = cache.query(query).getAll();
            long endTime = System.nanoTime();

            double milliseconds = (endTime - startTime) / 1_000_000.0;
            totalMs += milliseconds;

            System.out.printf("Query %d: %.2f ms (aggregate_id: %s, records: %d)%n",
                    i + 1, milliseconds, aggregateId, queryResults.size());
        }

        double avgMs = totalMs / repetitions;
        System.out.printf("%nSummary (Direct Queries):%n");
        System.out.printf("Total queries: %d%n", repetitions);
        System.out.printf("Average query time: %.2f ms%n", avgMs);
        System.out.printf("Total time: %.2f ms%n", totalMs);
    }

    public void runThroughputTest(TestMode testMode) throws InterruptedException {
        switch (testMode) {
            case REPEATED_AGGREGATE_QUERY:
                runRepeatedAggregateQuery(100);
                return;
            case REPEATED_MULTI_AGGREGATE_QUERY:
                runRepeatedMultiAggregateQuery(DEFAULT_QUERY_REPETITIONS);
                return;
            case COMPUTE_JOB:
                runComputeJobTest();
                return;
            default:
                // Handle other modes with existing logic
                AtomicInteger selectCount = new AtomicInteger(0);
                AtomicInteger updateCount = new AtomicInteger(0);
                CountDownLatch latch = new CountDownLatch(
                        testMode == TestMode.CONCURRENT ? 2 : 1
                );

                System.out.println("==================================================================================");
                System.out.println("Starting throughput test for " + testDurationSeconds +
                        " seconds in " + testMode + " mode...");

                if (testMode == TestMode.SELECT_ONLY || testMode == TestMode.CONCURRENT) {
                    runSelectThread(selectCount, latch);
                }

                if (testMode == TestMode.UPDATE_ONLY || testMode == TestMode.CONCURRENT) {
                    runUpdateThread(updateCount, latch);
                }

                latch.await();

                printResults(selectCount.get(), updateCount.get(), testMode);
        }
    }

    private void printResults(int totalSelects, int totalUpdates, TestMode testMode) {
        System.out.println("\nTest completed!");

        if (testMode == TestMode.SELECT_ONLY || testMode == TestMode.CONCURRENT) {
            System.out.println("==================================================================================");
            System.out.println("Select operations: " + totalSelects +
                    " (throughput: " + (totalSelects / testDurationSeconds) + " ops/sec)");
            System.out.println("==================================================================================");
        }

        if (testMode == TestMode.UPDATE_ONLY || testMode == TestMode.CONCURRENT) {
            System.out.println("==================================================================================");
            System.out.println("Update operations: " + totalUpdates +
                    " (throughput: " + (totalUpdates / testDurationSeconds) + " ops/sec)");
            System.out.println("==================================================================================");
        }

    }

    public void close() {
        if (ignite != null) {
            ignite.close();
        }
    }

    private static void printUsage() {
        System.out.println("Usage: java IgniteReadWriteThroughputChecker <configFile> [mode1] [mode2] ... [--compute]");
        System.out.println("Available modes: " + Arrays.toString(TestMode.values()));
        System.out.println("Options:");
        System.out.println("  --compute    Execute queries using compute jobs");
    }

    private static Set<TestMode> parseTestModes(String[] args) {
        Set<TestMode> modesToRun = new HashSet<>();
        if (args.length > 1) {
            for (int i = 1; i < args.length; i++) {
                try {
                    modesToRun.add(TestMode.valueOf(args[i].toUpperCase()));
                } catch (IllegalArgumentException e) {
                    System.out.println("Warning: Invalid mode '" + args[i] + "' ignored");
                }
            }
        } else {
            // If no modes specified, run all modes except POPULATE_ONLY
            modesToRun.addAll(Arrays.asList(TestMode.values()));
            modesToRun.remove(TestMode.POPULATE);
        }
        return modesToRun;
    }

    private void runComputeJobTest() {
        SqlFieldsQuery findAggregatesQuery = new SqlFieldsQuery(
                String.format("SELECT DISTINCT aggregate_id FROM %s", tableName));

        List<List<?>> results = cache.query(findAggregatesQuery).getAll();
        if (results.isEmpty()) {
            System.out.println("No aggregate IDs found in table!");
            return;
        }

        System.out.printf("Found %d aggregate IDs. Will execute compute jobs...%n", results.size());

        IgniteCompute compute = ignite.compute();
        Random random = new Random();
        double totalMs = 0;
        int totalRecords = 0;
        int successfulQueries = 0;
        int repetitions = DEFAULT_QUERY_REPETITIONS;

        System.out.println("\nExecuting compute jobs...");
        System.out.println("==================================================================================");

        for (int i = 0; i < repetitions; i++) {
            UUID aggregateId = (UUID) results.get(random.nextInt(results.size())).get(0);
            QueryExecutionJob job = new QueryExecutionJob(tableName, aggregateId);

            QueryExecutionJob.QueryExecutionResult result = compute.call(job);

            System.out.printf("%nJob %d (aggregate_id: %s):%n", i + 1, result.getAggregateId());
            if (result.isSuccess()) {
                System.out.printf("Total execution time: %.2f ms%n", result.getExecutionTimeMs());
                List<QueryExecutionJob.QueryResult> queryResults = result.getQueryResults();

                if (queryResults != null && !queryResults.isEmpty()) {
                    System.out.println("Individual query results:");
                    System.out.println("------------------------------------------");

                    for (int j = 0; j < queryResults.size(); j++) {
                        QueryExecutionJob.QueryResult qr = queryResults.get(j);
                        System.out.printf("Query %d: %.2f ms | Data: %s%n",
                                j + 1, qr.getExecutionTimeMs(), qr.getData());
                    }
                }

                totalMs += result.getExecutionTimeMs();
                totalRecords += result.getResultCount();
                successfulQueries++;
            } else {
                System.out.println("FAILED");
            }
            System.out.println("------------------------------------------");
        }

        System.out.println("==================================================================================");
        if (successfulQueries > 0) {
            double avgMs = totalMs / successfulQueries;
            System.out.printf("%nSummary:%n");
            System.out.printf("Total jobs executed:     %d%n", repetitions);
            System.out.printf("Successful jobs:         %d%n", successfulQueries);
            System.out.printf("Failed jobs:            %d%n", repetitions - successfulQueries);
            System.out.printf("Total records returned: %d%n", totalRecords);
            System.out.printf("Average job time:       %.2f ms%n", avgMs);
            System.out.printf("Total execution time:   %.2f ms%n", totalMs);
        } else {
            System.out.println("\nAll compute jobs failed!");
        }
    }

    /**
     * Compute job for executing SQL queries across Ignite cluster nodes.
     */
    private static class QueryExecutionJob implements IgniteCallable<QueryExecutionJob.QueryExecutionResult>, Serializable {
        private static final long serialVersionUID = 1L;

        private final String tableName;
        private final UUID aggregateId;

        @IgniteInstanceResource
        private transient Ignite ignite;

        public QueryExecutionJob(String tableName, UUID aggregateId) {
            this.tableName = tableName;
            this.aggregateId = aggregateId;
        }

        @Override
        public QueryExecutionResult call() {
            long startTime = System.nanoTime();
            List<QueryResult> queryResults = new ArrayList<>();

            try {
                IgniteCache<?, ?> cache = ignite.cache("TEST_CACHE1_" + tableName.toUpperCase());
                SqlFieldsQuery query = new SqlFieldsQuery(
                        String.format("SELECT * FROM %s WHERE aggregate_id = ?", tableName))
                        .setArgs(aggregateId);

                List<List<?>> results = cache.query(query).getAll();

                // Store individual query results
                for (List<?> row : results) {
                    queryResults.add(new QueryResult(
                            row,  // the actual row data
                            System.nanoTime() - startTime  // time since start in nanos
                    ));
                }

                long endTime = System.nanoTime();
                double milliseconds = (endTime - startTime) / 1_000_000.0;

                return new QueryExecutionResult(
                        aggregateId,
                        milliseconds,
                        results.size(),
                        true,
                        queryResults
                );
            } catch (Exception e) {
                System.err.println("Error executing query for aggregate_id " + aggregateId + ": " + e.getMessage());
                return new QueryExecutionResult(
                        aggregateId,
                        -1.0,
                        0,
                        false,
                        queryResults
                );
            }
        }

        /**
         * Individual query result with timing information
         */
        public static class QueryResult implements Serializable {
            private static final long serialVersionUID = 1L;

            private final List<?> data;
            private final long executionTimeNanos;

            public QueryResult(List<?> data, long executionTimeNanos) {
                this.data = data;
                this.executionTimeNanos = executionTimeNanos;
            }

            public List<?> getData() { return data; }
            public double getExecutionTimeMs() { return executionTimeNanos / 1_000_000.0; }
        }

        public static class QueryExecutionResult implements Serializable {
            private static final long serialVersionUID = 1L;

            private final UUID aggregateId;
            private final double executionTimeMs;
            private final int resultCount;
            private final boolean success;
            private final List<QueryResult> queryResults;

            public QueryExecutionResult(UUID aggregateId, double executionTimeMs,
                                        int resultCount, boolean success,
                                        List<QueryResult> queryResults) {
                this.aggregateId = aggregateId;
                this.executionTimeMs = executionTimeMs;
                this.resultCount = resultCount;
                this.success = success;
                this.queryResults = queryResults;
            }

            public UUID getAggregateId() { return aggregateId; }
            public double getExecutionTimeMs() { return executionTimeMs; }
            public int getResultCount() { return resultCount; }
            public boolean isSuccess() { return success; }
            public List<QueryResult> getQueryResults() { return queryResults; }
        }
    }

    public static void main(String[] args) {
        if (args.length < 1) {
            printUsage();
            return;
        }

        String tableName = "Event";
        int testDurationSeconds = 60;
        int initialRecordCount = 10000;

        IgniteReadWriteThroughputChecker checker = new IgniteReadWriteThroughputChecker(
                tableName, testDurationSeconds, initialRecordCount);

        try {
            checker.initialize(args[0]);
            Set<TestMode> modesToRun = parseTestModes(args);

            // Handle population mode
            if (modesToRun.contains(TestMode.POPULATE)) {
                checker.createAndPopulateTable();
                if (modesToRun.size() == 1) {
                    System.out.println("Population completed. Exiting as requested.");
                    return;
                }
            }

            // Run the requested test modes
            for (TestMode mode : modesToRun) {
                if (mode != TestMode.POPULATE) {
                    try {
                        checker.runThroughputTest(mode);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }
        } finally {
//            checker.close();
        }
    }
}
