package org.example;
import java.sql.*;
import java.io.File;
import java.nio.file.Files;
import java.nio.file.Paths;

public class ShardSimulation {

    // Shard URLs for multiple databases (insta1, insta2, etc.)
    private static final String[] SHARD_URLS = {
            "jdbc:h2:mem:insta1;DB_CLOSE_DELAY=-1;MODE=MySQL",
            "jdbc:h2:mem:insta2;DB_CLOSE_DELAY=-1;MODE=MySQL",
            "jdbc:h2:mem:insta3;DB_CLOSE_DELAY=-1;MODE=MySQL"
    };

    public static void main(String[] args) {
        long startTime = System.currentTimeMillis();
        // Step 1: Setup the Shards (Create tables)
        setupShards();
        long endTime = System.currentTimeMillis();
        System.out.println("Step 1 (Setup Shards) Time: " + (endTime - startTime) + "ms");

        startTime = System.currentTimeMillis();
        // Step 2: Alter the schema of one table (Post table in insta1)
        alterTableSchema();
        endTime = System.currentTimeMillis();
        System.out.println("Step 2 (Alter Schema) Time: " + (endTime - startTime) + "ms");

        startTime = System.currentTimeMillis();
        // Step 3: Insert data into shards (simulate posts, users, profiles)
        insertDataIntoShards();
        endTime = System.currentTimeMillis();
        System.out.println("Step 3 (Insert Data) Time: " + (endTime - startTime) + "ms");

        startTime = System.currentTimeMillis();
        // Step 4: Dump Data from one shard and load it into another using H2 SQL Dump utility
        dumpAndLoadData();
        endTime = System.currentTimeMillis();
        System.out.println("Step 4 (Dump & Load Data) Time: " + (endTime - startTime) + "ms");
    }

    // Step 1: Setup Shards (Create tables in all physical shards)
    private static void setupShards() {
        System.out.println("Setting up shards...");
        for (int i = 0; i < SHARD_URLS.length; i++) {
            try (Connection conn = DriverManager.getConnection(SHARD_URLS[i], "sa", "")) {
                Statement stmt = conn.createStatement();
                stmt.execute("CREATE TABLE IF NOT EXISTS users (ID INT PRIMARY KEY, Name VARCHAR(255))");
                stmt.execute("CREATE TABLE IF NOT EXISTS posts (ID INT PRIMARY KEY, UserID INT, Content VARCHAR(255))");
                stmt.execute("CREATE TABLE IF NOT EXISTS profile (UserID INT PRIMARY KEY, Bio VARCHAR(255))");
                System.out.println("Shard " + (i + 1) + " setup complete.");
            } catch (SQLException e) {
                System.err.println("Error setting up Shard " + (i + 1) + ": " + e.getMessage());
            }
        }
    }

    // Step 2: Alter schema in one table (Add a column to 'posts' in insta1)
    private static void alterTableSchema() {
        System.out.println("Altering schema in shard 1 (insta1)...");
        try (Connection conn = DriverManager.getConnection(SHARD_URLS[0], "sa", "")) {
            Statement stmt = conn.createStatement();
            stmt.execute("ALTER TABLE posts ADD COLUMN PostDate TIMESTAMP");
            System.out.println("Schema altered in shard 1 (insta1).");
        } catch (SQLException e) {
            System.err.println("Error altering schema: " + e.getMessage());
        }
    }

    // Step 3: Insert sample data into shards
    private static void insertDataIntoShards() {
        System.out.println("Inserting data into shards...");
        for (int i = 1; i <= 20; i++) {
            String name = "User" + i;
            String content = "Post content for user " + i;

            // Insert into users table and posts table
            String shardUrl = (i % 2 == 0) ? SHARD_URLS[1] : SHARD_URLS[0]; // Distribute by ID (even/odd)
            try (Connection conn = DriverManager.getConnection(shardUrl, "sa", "")) {
                Statement stmt = conn.createStatement();
                stmt.execute("INSERT INTO users (ID, Name) VALUES (" + i + ", '" + name + "')");
                stmt.execute("INSERT INTO posts (ID, UserID, Content) VALUES (" + (i * 10) + ", " + i + ", '" + content + "')");
                stmt.execute("INSERT INTO profile (UserID, Bio) VALUES (" + i + ", 'Bio for user " + i + "')");
                System.out.println("Inserted data for User " + i + " into " + shardUrl);
            } catch (SQLException e) {
                System.err.println("Error inserting data for User " + i + ": " + e.getMessage());
            }
        }
    }

    // Step 4: Dump Data from one shard and load it into another (using H2 SQL Dump utility)
    private static void dumpAndLoadData() {
        System.out.println("Dumping and loading data between shards...");
        try {
            // Specify file path for the dump
            String dumpFilePath = "insta1_dump.sql";
            File dumpFile = new File(dumpFilePath);
            if (dumpFile.exists()) {
                Files.delete(Paths.get(dumpFilePath));  // Clean up previous dump if exists
            }

            // Dump data from shard 1 to a file
            String dumpCommand = "SCRIPT TO '" + dumpFilePath + "'";
            long startTime = System.currentTimeMillis();
            executeSQLCommand(SHARD_URLS[0], dumpCommand);
            long endTime = System.currentTimeMillis();
            System.out.println("Data dumped from Shard 1 in " + (endTime - startTime) + "ms");

            // Load data into shard 2 from the dump file
            String loadCommand = "RUNSCRIPT FROM '" + dumpFilePath + "'";
            startTime = System.currentTimeMillis();
            executeSQLCommand(SHARD_URLS[1], loadCommand);
            endTime = System.currentTimeMillis();
            System.out.println("Data loaded into Shard 2 in " + (endTime - startTime) + "ms");

            System.out.println("Data dumped from Shard 1 and loaded into Shard 2.");
        } catch (SQLException e) {
            System.err.println("Error during dump/load: " + e.getMessage());
        } catch (Exception e) {
            System.err.println("Error handling dump file: " + e.getMessage());
        }
    }

    // Method to execute SQL command (e.g., dump/load)
    private static void executeSQLCommand(String shardUrl, String command) throws SQLException {
        try (Connection conn = DriverManager.getConnection(shardUrl, "sa", "")) {
            Statement stmt = conn.createStatement();
            stmt.execute(command);
        }
    }
}
