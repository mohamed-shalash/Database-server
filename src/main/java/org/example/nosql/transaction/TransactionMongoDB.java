package org.example.nosql.transaction;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.example.nosql.transaction.Utils.*;
import org.example.nosql.transaction.Utils.Collections;
import org.example.nosql.transaction.exceptions.TransactionException;
import org.example.nosql.transaction.transaction.Transaction;
import org.example.nosql.transaction.transaction.TransactionManager;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

public class TransactionMongoDB {
    private static final int PORT = 27017;
    private static String currentDatabase = "test";

    private static Map<String, Database> databases = new ConcurrentHashMap<>();
    private static Map<String, Database> transaction = new ConcurrentHashMap<>();
    private static ScheduledExecutorService scheduler;
    private static Utils utils=new Utils();

    public static void main(String[] args) {
        databases.putAll(utils.loadDatabases());
        startServer();
    }

    private static void startServer() {
        try (ServerSocket serverSocket = new ServerSocket(PORT)) {
            System.out.println("Persistent MongoDB-like server started on port " + PORT);

            // Setup periodic saves every 5 minutes
            scheduler = Executors.newScheduledThreadPool(1);
            // scheduler.scheduleAtFixedRate(PersistentMongoDB::saveDatabases, 5, 5, TimeUnit.MINUTES);

            // Add shutdown hook for clean exit
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                scheduler.shutdown();
                utils.saveDatabases(databases);
                System.out.println("Server shutdown complete");
            }));

            while (true) {
                Socket clientSocket = serverSocket.accept();
                new Thread(new ClientHandler(clientSocket)).start();
            }
        } catch (IOException e) {
            System.err.println("Server exception: " + e.getMessage());
        }
    }


    private static class ClientHandler implements Runnable  {
        private final Socket clientSocket;


        public ClientHandler(Socket socket) {
            this.clientSocket = socket;
        }

        @Override
        public void run() {
            try (
                    BufferedReader in = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
                    PrintWriter out = new PrintWriter(clientSocket.getOutputStream(), true);
            ) {
                String inputLine;

                while ((inputLine = in.readLine()) != null) {
                    System.out.println(inputLine);
                    String response = processCommand(inputLine);
                    out.println(response);
                }
            } catch (IOException e) {
                System.err.println("Client handler exception: " + e.getMessage());
            } finally {
                try { clientSocket.close(); } catch (IOException ignored) {}
            }
        }
        final TransactionManager transactionManager = new TransactionManager();
        Long currentTransactionId = null;
        private String processCommand(String command) throws JsonProcessingException {
            try {


                switch (command.toUpperCase()) {

                    case "BEGIN":
                        if (currentTransactionId != null) return "Transaction already active";
                        transaction =new ConcurrentHashMap<>(databases);
                        currentTransactionId = transactionManager.beginTransaction();
                        return "Transaction ID: " + currentTransactionId;

                    case "COMMIT":
                        if (currentTransactionId == null) return "No active transaction";
                        transactionManager.commitTransaction(currentTransactionId,transaction);
                        databases =new ConcurrentHashMap<>(transaction);
                        currentTransactionId = null;
                        return "Transaction committed";

                    case "ROLLBACK":
                        if (currentTransactionId == null) return "No active transaction";
                        transactionManager.rollbackTransaction(currentTransactionId);
                        transaction =new ConcurrentHashMap<>();
                        currentTransactionId = null;
                        return "Transaction rolled back";
                }

                Transaction transaction = currentTransactionId != null ?
                        transactionManager.getTransaction(currentTransactionId) : null;

                String[] words = command.split("\\s+", 3);
                String operation = words[0].toUpperCase(); // "insert" or "update"
                String tbl = words.length>1?words[1]:""; // "tbl"
                List<String> jsonObjects = extractJsonObjects(command);new ArrayList<>();

                // Handle other commands with transaction context
                switch (operation) {
                    case "USE":
                        currentDatabase = words[1];
                        return "Switched to database " + currentDatabase;

                    case "INSERT":
                        return handleInsert(tbl, jsonObjects.get(0), transaction);

                    case "FIND":
                        return handleFind(tbl, jsonObjects.size() > 0 ? jsonObjects.get(0) : "{}");

                    case "UPDATE":
                        jsonObjects = extractJsonObjects(command);
                        return handleUpdate(words[1], jsonObjects.get(0), jsonObjects.get(1), transaction);
                    case "DELETE":
                        return handleDelete(tbl, jsonObjects.get(0),transaction);
                    case "CREATE_INDEX":
                        return createIndex(tbl, words[2],transaction);
                    case "DROP":
                        return dropCollection(tbl,transaction);
                    // Modify other command handlers similarly

                    default:
                        return "ERROR: Unknown command";
                }
            } catch (TransactionException e) {
                currentTransactionId = null;
                return "TX ERROR: " + e.getMessage();
            }catch (Exception e){
                return "unValid command";
            }




        }

        private List<String> extractJsonObjects(String command) {
            List<String> jsonObjects = new ArrayList<>();
            int openBraces = 0;
            StringBuilder currentJson = new StringBuilder();

            for (char c : command.toCharArray()) {
                if (c == '{') {
                    openBraces++;
                }

                if (openBraces > 0) {
                    currentJson.append(c);
                }

                if (c == '}') {
                    openBraces--;

                    // If we closed all braces, we have a full JSON object
                    if (openBraces == 0) {
                        jsonObjects.add(currentJson.toString().trim());
                        currentJson.setLength(0); // Reset for the next JSON object
                    }
                }
            }

            return jsonObjects;
        }


        private String handleInsert(String collectionName, String documentStr, Transaction tx)
                throws JsonProcessingException {

            Database db = databases.computeIfAbsent(currentDatabase, k -> new Database());
            Collections collection = db.getCollections().computeIfAbsent(collectionName, k -> new Collections());

            Map<String, Object> documentData = parseJson(documentStr);
            documentData.forEach((key, value) -> {
                System.out.println("Key: " + key + ", Value: " + value);
            });
            Document doc = new Document(documentData);

            if (tx != null) {
                tx.addOperation(new Transaction.Operation(
                        Transaction.OperationType.INSERT,
                        collectionName,
                        doc.getId(),
                        documentData
                ));
                //return "Document staged for insert in transaction";
                transaction.computeIfAbsent(currentDatabase, k -> new Database())
                        .getCollections().computeIfAbsent(collectionName, k -> new Collections())
                        .getDocuments().put(doc.getId(), doc);
                return "Document staged for insert in transaction";
            }

            collection.getDocuments().put(doc.getId(), doc);
            updateIndexes(collection, doc);
            return "Inserted document ID: " + doc.getId();
        }

        private String handleUpdate(String collectionName, String queryStr, String updateStr, Transaction tx) throws JsonProcessingException {


            Map<String, Object> query = parseJson(queryStr);//parseDocument(queryStr);
            Map<String, Object> update = parseJson(updateStr);
            int updatedCount = 0;
            if (tx != null) {
                Collections collection = getTransactionCollection(collectionName);
                if (collection == null) return "Collection not found";

                for (Document doc : collection.getDocuments().values()) {
                    tx.addOperation(new Transaction.Operation(
                            Transaction.OperationType.UPDATE,
                            collectionName,
                            doc.getId(),
                            update
                    ));
                    if (matchesQuery(doc.getData(), query)) {
                        doc.getData().putAll(update);

                        updatedCount++;
                    }
                }
                return "Commit Updated " + updatedCount + " documents";
            }else {
                Collections collection = getCollection(collectionName);
                if (collection == null) return "Collection not found";

                for (Document doc : collection.getDocuments().values()) {
                    if (matchesQuery(doc.getData(), query)) {
                        doc.getData().putAll(update);

                        updatedCount++;
                    }
                }
                return "Updated " + updatedCount + " documents";
            }
        }

        private String handleFind(String collectionName, String queryStr) throws JsonProcessingException {
            Collections collection = getCollection(collectionName);
            if (collection == null) return "Collection not found";

            Map<String, Object> query = parseJson(queryStr);
            StringBuilder result = new StringBuilder();

            collection.getDocuments().values().stream()
                    .filter(doc -> matchesQuery(doc.getData(), query))
                    .forEach(doc -> result.append(doc.getData()).append("\n"));

            return result.length() > 0 ? result.toString() : "No documents found";
        }


        private String handleDelete(String collectionName, String queryStr, Transaction tx) throws JsonProcessingException {


            Map<String, Object> query = parseJson(queryStr);
            int deletedCount = 0;

            if (tx != null) {
                Collections collection = getTransactionCollection(collectionName);
                if (collection == null) return "Collection not found";

                Iterator<Map.Entry<String, Document>> it = collection.getDocuments().entrySet().iterator();
                while (it.hasNext()) {
                    Document doc = it.next().getValue();

                    if (matchesQuery(doc.getData(), query)) {
                        it.remove();
                        deletedCount++;
                    }
                }
                return "Commit Deleted " + deletedCount + " documents";
            }
            else {
                Collections collection = getCollection(collectionName);
                if (collection == null) return "Collection not found";
                Iterator<Map.Entry<String, Document>> it = collection.getDocuments().entrySet().iterator();
                while (it.hasNext()) {
                    Document doc = it.next().getValue();

                    if (matchesQuery(doc.getData(), query)) {
                        it.remove();
                        deletedCount++;
                    }
                }
                return "Deleted " + deletedCount + " documents";
            }
        }

        private String createIndex(String collectionName, String field,Transaction tx) {

            Collections collection = getCollection(collectionName);
            if (collection == null) return "Collection not found";

            if (tx != null) {
                collection = getTransactionCollection(collectionName);
                if (collection == null) return "Collection not found";

                collection.getIndexes().computeIfAbsent(field, k -> ConcurrentHashMap.newKeySet());
                for (Document doc : collection.getDocuments().values()) {
                    if (doc.getData().containsKey(field)) {
                        collection.getIndexes().get(field).add(doc.getData().get(field).toString());
                    }
                }

                return "transaction Index created on " + field;
            }
            collection = getCollection(collectionName);
            if (collection == null) return "Collection not found";
            collection.getIndexes().computeIfAbsent(field, k -> ConcurrentHashMap.newKeySet());
            for (Document doc : collection.getDocuments().values()) {
                if (doc.getData().containsKey(field)) {
                    collection.getIndexes().get(field).add(doc.getData().get(field).toString());
                }
            }
            return "Index created on " + field;
        }

        private String dropCollection(String collectionName,Transaction tx) {
            Database db;
            if (tx != null) {
                db = transaction.get(currentDatabase);
                if (db == null) return "Database not found";
                db.getCollections().remove(collectionName);
                return "Collection dropped";
            }
            db = databases.get(currentDatabase);
            if (db == null) return "Database not found";
            db.getCollections().remove(collectionName);
            return "Collection dropped";
        }


        private boolean matchesQuery(Map<String, Object> document, Map<String, Object> query) {
            return query.entrySet().stream()
                    .allMatch(entry -> {
                        Object docValue = document.get(entry.getKey());
                        Object queryValue = entry.getValue();

                        if (queryValue instanceof Map) {
                            return handleOperators(docValue, (Map<String, Object>) queryValue);
                        }
                        return Objects.equals(docValue, queryValue);
                    });
        }

        private boolean handleOperators(Object docValue, Map<String, Object> operators) {
            return operators.entrySet().stream().allMatch(op -> {
                String operator = op.getKey();
                Object value = op.getValue();

                switch (operator) {
                    case "$gt":
                        return compare(docValue, value) > 0;
                    case "$lt":
                        return compare(docValue, value) < 0;
                    case "$in":
                        return ((Collections) value).getDocuments().contains(docValue);
                    default:
                        return false;
                }
            });
        }
        private int compare(Object a, Object b) {
            if (a instanceof Comparable && b instanceof Comparable) {
                return ((Comparable) a).compareTo(b);
            }
            return 0;
        }
        private Collections getCollection(String collectionName) {
            try {
                Database db = databases.get(currentDatabase);
                return (db != null) ? db.getCollections().get(collectionName) : null;
            }catch (Exception e){
                return new Collections();
            }

        }

        private Collections getTransactionCollection(String collectionName) {
            try {
                Database db = transaction.get(currentDatabase);
                return (db != null) ? db.getCollections().get(collectionName) : null;
            }catch (Exception e){
                return new Collections();
            }

        }

        private Map<String,Object> parseJson(String input) throws JsonProcessingException {
            System.out.println(input);
            String json = input
                    .replaceAll("([a-zA-Z0-9_]+)\\s*:", "\"$1\":") // Quote keys
                    .replaceAll(":\\s*([a-zA-Z_]+)(?=[,}])", ":\"$1\""); // Quote only non-number values

            System.out.println("Normalized JSON: " + json);

            ObjectMapper objectMapper = new ObjectMapper();
            return objectMapper.readValue(json, Map.class);
        }

        private void updateIndexes(Collections collection, Document doc) {
            for (String indexedField : collection.getIndexes().keySet()) {
                if (doc.getData().containsKey(indexedField)) {
                    collection.getIndexes().get(indexedField).add(doc.getData().get(indexedField).toString());
                }
            }
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
 * commit index
 * handle transacton in transacton
 * clean code
 * save indexes
 * add join command
 * use pages
 * make it in production stuff
 *
 */