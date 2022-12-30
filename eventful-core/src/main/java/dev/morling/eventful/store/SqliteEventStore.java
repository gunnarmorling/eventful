package dev.morling.eventful.store;

import java.io.IOException;
import java.lang.System.Logger;
import java.lang.System.Logger.Level;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.sqlite.SQLiteConfig;
import org.sqlite.SQLiteErrorCode;
import org.sqlite.SQLiteException;

public class SqliteEventStore implements EventStore {

    private static final int QUEUE_SIZE = 16384 * 2;

    private static Logger LOGGER = System.getLogger(SqliteEventStore.class.getName());

    private final Connection conn;
    private final Connection writeConn;
    private final Map<String, PreparedStatement> insertStatements;
    private final Map<String, PreparedStatement> maxVersionStatements;
    private final BlockingQueue<Work> workQueue;
    private final ExecutorService executorService;
    private volatile boolean running;

    private static record Work(String streamType, Event event, CompletableFuture<Void> future, PreparedStatement insertStatement, PreparedStatement maxVersionStatement) {
    }

    public SqliteEventStore() {
        try {
            if (Files.exists(Path.of("test.db"))) {
                Files.delete(Path.of("test.db"));
            }

            writeConn = DriverManager.getConnection("jdbc:sqlite:test.db");

            try(Statement statement = writeConn.createStatement()) {
                statement.executeUpdate("pragma journal_mode=wal");
            }

            writeConn.setAutoCommit(false);

            SQLiteConfig config = new SQLiteConfig();
            config.setReadOnly(true);
            conn = DriverManager.getConnection("jdbc:sqlite:test.db", config.toProperties());

            try(Statement statement = conn.createStatement()) {
                statement.executeUpdate("pragma journal_mode=wal");
            }

            conn.setAutoCommit(false);
        }
        catch (SQLException | IOException e) {
            throw new IllegalStateException("Couldn't open database", e);
        }

        insertStatements = new HashMap<>();
        maxVersionStatements = new HashMap<>();
        workQueue = new ArrayBlockingQueue<>(QUEUE_SIZE);

        running = true;
        executorService = Executors.newFixedThreadPool(1);
        executorService.submit(() -> {
            pollWork();
        });
    }

    @Override
    public void close() {
        running = false;

        executorService.shutdown();
        try {
            executorService.awaitTermination(5_000, TimeUnit.SECONDS);
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            LOGGER.log(Level.WARNING, "Failed to await termination of executor service");
        }

        try {
            for (PreparedStatement statement : insertStatements.values()) {
                statement.close();
            }
            for (PreparedStatement statement : maxVersionStatements.values()) {
                statement.close();
            }

            writeConn.close();
            conn.close();
        }
        catch (SQLException e) {
            throw new IllegalStateException("Couldn't close database", e);
        }
    }

    @Override
    public void initialize(String streamType) {
        try {
            PreparedStatement statement = writeConn.prepareStatement("SELECT name FROM sqlite_master WHERE type='table' AND name=?");
            statement.setString(1, streamType);
            ResultSet rs = statement.executeQuery();
            if (rs.next()) {
                throw new IllegalArgumentException("Stream type '%s' already exists".formatted(streamType));
            }
            rs.close();
            statement.close();

            Statement tableStatement = writeConn.createStatement();
            tableStatement.executeUpdate("""
                    CREATE TABLE %s (
                    ID           TEXT PRIMARY KEY NOT NULL,
                    TYPE         TEXT             NOT NULL,
                    AGGREGATE_ID TEXT             NOT NULL,
                    VERSION      BIGINT           NOT NULL,
                    PAYLOAD      TEXT             NOT NULL
                  )
                  """.formatted(streamType));

            tableStatement.executeUpdate("CREATE UNIQUE INDEX %s_idx ON %s (AGGREGATE_ID, VERSION)".formatted(streamType, streamType));

//            Statement tableStatement = writeConn.createStatement();
//            tableStatement.executeUpdate("""
//                    CREATE TABLE %s (
//                    AGGREGATE_ID TEXT   NOT NULL,
//                    VERSION      BIGINT NOT NULL,
//                    ID           TEXT   NOT NULL,
//                    TYPE         TEXT   NOT NULL,
//                    PAYLOAD      TEXT   NOT NULL,
//                    PRIMARY KEY (AGGREGATE_ID, VERSION)
//                  ) WITHOUT ROWID;
//                  """.formatted(streamType));

            writeConn.commit();
            tableStatement.close();

//            awaitTable(streamType, conn);

            PreparedStatement insertStatement = writeConn.prepareStatement("""
                    INSERT INTO %s
                      (ID, TYPE, AGGREGATE_ID, VERSION, PAYLOAD)
                      VALUES (?, ?, ?, ?, ?)
                    """.formatted(streamType));

            insertStatements.put(streamType, insertStatement);

            PreparedStatement maxVersionStatement = writeConn.prepareStatement("""
                    SELECT MAX(VERSION) as MAX_VERSION
                    FROM %s
                    WHERE AGGREGATE_ID = ?
                    """.formatted(streamType));

            maxVersionStatements.put(streamType, maxVersionStatement);
        }
        catch (SQLException e) {
            throw new IllegalStateException("Couldn't query database", e);
        }
    }

//    private void awaitTable(String tableName, Connection connection) throws SQLException {
//        while(true) {
//            try(PreparedStatement statement = connection.prepareStatement("SELECT name FROM sqlite_master WHERE type='table' AND name=?")) {
//                statement.setString(1, tableName);
//                try(ResultSet rs = statement.executeQuery()) {
//                    if (rs.next()) {
//                        break;
//                    }
//                }
//            }
//        }
//    }

    @Override
    public Future<Void> persist(String streamType, Event event) {
        if (!running) {
            return CompletableFuture.failedFuture(new IllegalStateException("Store is stopped already"));
        }

        PreparedStatement insertStatement = insertStatements.get(streamType);

        if (insertStatement == null) {
            return CompletableFuture.failedFuture(new IllegalArgumentException("Stream type '%s' doesn't exist".formatted(streamType)));
        }

        PreparedStatement maxVersionStatement = maxVersionStatements.get(streamType);

        CompletableFuture<Void> future = new CompletableFuture<>();

        boolean enqueued = false;
        while (!enqueued) {
            enqueued = workQueue.offer(new Work(streamType, event, future, insertStatement, maxVersionStatement));
        }

        return future;
    }

    @Override
    public List<Event> events(String streamType, UUID aggregateId) {
        try {
            conn.commit();

            PreparedStatement statement = conn.prepareStatement("SELECT * FROM %s WHERE AGGREGATE_ID = ? ORDER BY VERSION".formatted(streamType));
            statement.setString(1, aggregateId.toString());

            ResultSet rs = statement.executeQuery();
            List<Event> events = new ArrayList<>();

            while ( rs.next() ) {
                String id = rs.getString("id");
                String type = rs.getString("type");
                long version = rs.getLong("version");
                String payload = rs.getString("payload");

                events.add(new Event(type, UUID.fromString(id), aggregateId, version, payload));
            }
            rs.close();
            statement.close();

            conn.commit();

            return events;
        }
        catch (SQLException e) {
            throw new IllegalStateException("Couldn't query database", e);
        }
    }

    private void pollWork() {
        while(running || !workQueue.isEmpty()) {
            List<Work> workItems = new ArrayList<>();
            int transferred = workQueue.drainTo(workItems);
            if (transferred == 0) {
                try {
                    Thread.sleep(10);
                    continue;
                }
                catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    LOGGER.log(Level.WARNING, "Interrupted while sleeping");
                }
            }
            else {
                execute(workItems);
            }
        }
    }

    private void execute(List<Work> workItems) {
        LOGGER.log(Level.INFO, "Processing " + workItems.size() + " work items");
        boolean allSuccessful = true;
        Work failed = null;

        for (Work work : workItems) {
            allSuccessful &= execute(work);

            if (!allSuccessful) {
                failed = work;
                break;
            }
        }

        if (allSuccessful) {
            try {
                writeConn.commit();
            }
            catch (SQLException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }

            for (Work work : workItems) {
                work.future().complete(null);
            }
        }
        else {
            try {
                writeConn.rollback();
            }
            catch (SQLException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }

            for (Work work : workItems) {
                if (work != failed) {
                    work.future().completeExceptionally(new RuntimeException("Other item in batch failed; try again"));
                }
            }
        }
    }

    private boolean execute(Work workItem) {
        String streamType = workItem.streamType();
        Event event = workItem.event();

        try {
            PreparedStatement maxVersionStatement = workItem.maxVersionStatement();
            PreparedStatement insertStatement = workItem.insertStatement();

            maxVersionStatement.setString(1, event.aggregateId().toString());
            ResultSet rs = maxVersionStatement.executeQuery();
            if (rs.next()) {
                long maxVersion = rs.getLong("MAX_VERSION");

                if (event.version() < maxVersion) {
                    workItem.future().completeExceptionally(new InconsistentVersionException(streamType, event.aggregateId(), event.version()));
                    return false;
                }
            }

            insertStatement.setString(1, event.id().toString());
            insertStatement.setString(2, event.type());
            insertStatement.setString(3, event.aggregateId().toString());
            insertStatement.setLong(4, event.version());
            insertStatement.setString(5, event.payload());

            insertStatement.executeUpdate();

            return true;
        }
        catch(SQLiteException e) {
            if (e.getResultCode() == SQLiteErrorCode.SQLITE_CONSTRAINT_UNIQUE) {
                workItem.future().completeExceptionally(new DuplicateVersionException(streamType, event.aggregateId(), event.version()));
            }
            else {
                workItem.future().completeExceptionally(new IllegalStateException("Couldn't persist event", e));
            }

            return false;
        }
        catch (SQLException e) {
            workItem.future().completeExceptionally(new IllegalStateException("Couldn't persist event", e));
            return false;
        }
    }
}
