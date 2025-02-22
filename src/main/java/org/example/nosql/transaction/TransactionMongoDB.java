package org.example.nosql.transaction;


import org.example.nosql.transaction.Utils.*;
import org.example.nosql.transaction.handler.ClientHandler;
import org.example.nosql.transaction.structure.Database;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

public class TransactionMongoDB {
    private static final int PORT = 27017;

    private static Map<String, Database> databases = new ConcurrentHashMap<>();
    private static ScheduledExecutorService scheduler;
    private static SaveData saveData=new SaveData();

    public static void main(String[] args) {
        startServer();
    }

    private static void startServer() {
        try (ServerSocket serverSocket = new ServerSocket(PORT)) {
            System.out.println("Persistent MongoDB-like server started on port " + PORT);

            // Setup periodic saves every 5 minutes
            scheduler = Executors.newScheduledThreadPool(1);
            /*Timer timer = new Timer();
            timer.schedule(new TimerTask() {
                @Override
                public void run() {
                    Utils.saveDatabases(databases);
                }
            }, 300_000, 300_000);*/

            databases.putAll(saveData.loadDatabases());

            // Add shutdown hook for clean exit
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                scheduler.shutdown();
                saveData.saveDatabases(databases);
                System.out.println("Server shutdown complete");
            }));

            while (true) {
                Socket clientSocket = serverSocket.accept();
                new Thread(new ClientHandler(clientSocket,databases)).start();
            }
        } catch (IOException e) {
            System.err.println("Server exception: " + e.getMessage());
        }
    }



}

/**
 * BEGIN
 * INSERT users { "name": "Alice", "balance": 100 }
 * UPDATE accounts { "id": 123 } { "balance": 50 }
 * COMMIT
 */

/**
 * next steps :
 *
 * clean code
 * save indexes in seperate database
 * commit index
 *
 * clean code
 * add view and materialized view
 * add readme file
 * use pages
 * make it in production stuff
 *  Implement WAL for crash recovery
 *  Add connection pooling
 *  Implement proper query parser
 *  Add replication support
 * Implement MVCC for better concurrency
 * add sql life cycle
 */

/**
 * tmp join
 * private String handleFindWithJoin(String joinType, String primaryCollection, String secondaryCollection, String field1, String field2, String queryStr) throws JsonProcessingException {
 *         Collections primary = getCollection(primaryCollection);
 *         Collections secondary = getCollection(secondaryCollection);
 *
 *         if (primary == null || secondary == null) {
 *             return "ERROR: One or both collections not found.";
 *         }
 *
 *         Map<String, Object> query = parseJson(queryStr);
 *         StringBuilder result = new StringBuilder();
 *         List<Map<String, Object>> mergedResults = new ArrayList<>();
 *         Set<Object> matchedKeys = new HashSet<>();
 *
 *         Map<Object, Map<String, Object>> primaryMap = new HashMap<>();
 *         for (Document doc1 : primary.getDocuments().values()) {
 *             primaryMap.put(doc1.getData().get(field1), new HashMap<>(doc1.getData()));
 *         }
 *
 *         for (Document doc2 : secondary.getDocuments().values()) {
 *             Object joinValue2 = doc2.getData().get(field2);
 *             if (primaryMap.containsKey(joinValue2)) {
 *                 Map<String, Object> mergedData = new HashMap<>(primaryMap.get(joinValue2));
 *                 mergedData.putAll(doc2.getData());
 *                 matchedKeys.add(joinValue2);
 *                 mergedResults.add(mergedData);
 *             } else if (joinType.equalsIgnoreCase("RIGHT")) {
 *                 Map<String, Object> rightJoinData = new HashMap<>(doc2.getData());
 *                 primary.getDocuments().values().stream().findFirst().ifPresent(doc ->
 *                         doc.getData().keySet().forEach(key -> rightJoinData.putIfAbsent(key, null))
 *                 );
 *                 mergedResults.add(rightJoinData);
 *             }
 *         }
 *
 *         if (joinType.equalsIgnoreCase("LEFT")) {
 *             for (Map.Entry<Object, Map<String, Object>> entry : primaryMap.entrySet()) {
 *                 if (!matchedKeys.contains(entry.getKey())) {
 *                     Map<String, Object> leftJoinData = new HashMap<>(entry.getValue());
 *                     secondary.getDocuments().values().stream().findFirst().ifPresent(doc ->
 *                             doc.getData().keySet().forEach(key -> leftJoinData.putIfAbsent(key, null))
 *                     );
 *                     mergedResults.add(leftJoinData);
 *                 }
 *             }
 *         }
 *
 *         for (Map<String, Object> mergedData : mergedResults) {
 *             if (matchesQuery(mergedData, query)) {
 *                 result.append(mergedData).append("\n");
 *             }
 *         }
 *
 *         return result.length() > 0 ? result.toString() : "No matching records found.";
 *     }
 */