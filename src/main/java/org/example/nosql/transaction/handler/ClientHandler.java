package org.example.nosql.transaction.handler;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.example.nosql.transaction.Utils.Collections;
import org.example.nosql.transaction.Utils.Database;
import org.example.nosql.transaction.Utils.Document;
import org.example.nosql.transaction.exceptions.TransactionException;
import org.example.nosql.transaction.transaction.Transaction;
import org.example.nosql.transaction.transaction.TransactionManager;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class ClientHandler implements Runnable  {
    private final Socket clientSocket;
    private static String currentDatabase = "test";

    //private static Map<String, Database> transaction = new ConcurrentHashMap<>();
    private static Map<String, Database> databases = new ConcurrentHashMap<>();

    public ClientHandler(Socket socket, Map<String, Database> databases) {
        this.databases=databases;
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
                    currentTransactionId = transactionManager.beginTransaction();
                    transactionManager.setDatabase(currentTransactionId,databases);
                    return "Transaction ID: " + currentTransactionId;

                case "COMMIT":
                    if (currentTransactionId == null) return "No active transaction";
                    transactionManager.commitTransaction(currentTransactionId,transactionManager.getDatabase(currentTransactionId));
                    databases =new ConcurrentHashMap<>(transactionManager.getDatabase(currentTransactionId));
                    transactionManager.removeTransaction(currentTransactionId);
                    currentTransactionId = null;
                    return "Transaction committed";

                case "ROLLBACK":
                    if (currentTransactionId == null) return "No active transaction";
                    transactionManager.rollbackTransaction(currentTransactionId);
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
                    if (words.length > 2 && words[1].equalsIgnoreCase("JOIN")) {
                        String joinType = words[2].toUpperCase(); // INNER, LEFT, RIGHT
                        String table1 = words[3];
                        String table2 = words[4];
                        String[] joinCondition = words[6].split("="); // Extract field names

                        if (joinCondition.length != 2) return "ERROR: Invalid JOIN syntax.";

                        // Extract JSON filter (if provided)
                        String queryJson = jsonObjects.size() > 0 ? jsonObjects.get(0) : "{}";

                        return handleFindWithJoin(
                                joinType,
                                table1, table2,
                                joinCondition[0].trim(), joinCondition[1].trim(),
                                queryJson
                        );
                    }
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
            System.out.println(e);
            return "unValid command";
        }
    }


    private String handleFindWithJoin(String joinType, String primaryCollection, String secondaryCollection, String field1, String field2, String queryStr) throws JsonProcessingException {
        Collections primary = getCollection(primaryCollection);
        Collections secondary = getCollection(secondaryCollection);

        if (primary == null || secondary == null) {
            return "ERROR: One or both collections not found.";
        }

        Map<String, Object> query = parseJson(queryStr);
        StringBuilder result = new StringBuilder();

        Set<Object> matchedRightSideKeys = new HashSet<>(); // Track matched right-side records

        // Process LEFT JOIN & INNER JOIN
        for (Document doc1 : primary.getDocuments().values()) {
            Object joinValue1 = doc1.getData().get(field1);
            boolean matched = false;

            for (Document doc2 : secondary.getDocuments().values()) {
                Object joinValue2 = doc2.getData().get(field2);
                if (joinValue1 != null && joinValue1.equals(joinValue2)) {
                    matchedRightSideKeys.add(doc2.getId()); // Mark as matched
                    Map<String, Object> mergedData = new HashMap<>(doc1.getData());
                    mergedData.putAll(doc2.getData()); // Merge records

                    if (matchesQuery(mergedData, query)) {
                        result.append(mergedData).append("\n");
                    }
                    matched = true;
                }
            }

            // LEFT JOIN: If no match, still include left table data with null right-side fields
            if (!matched && joinType.equals("LEFT")) {
                Map<String, Object> leftJoinData = new HashMap<>(doc1.getData());
                for (String key : secondary.getDocuments().values().stream().findFirst().get().getData().keySet()) {
                    leftJoinData.putIfAbsent(key, null); // Fill missing fields with null
                }
                if (matchesQuery(leftJoinData, query)) {
                    result.append(leftJoinData).append("\n");
                }
            }
        }

        // RIGHT JOIN: Include unmatched records from the right table
        if (joinType.equals("RIGHT")) {
            for (Document doc2 : secondary.getDocuments().values()) {
                if (!matchedRightSideKeys.contains(doc2.getId())) {
                    Map<String, Object> rightJoinData = new HashMap<>(doc2.getData());
                    for (String key : primary.getDocuments().values().stream().findFirst().get().getData().keySet()) {
                        rightJoinData.putIfAbsent(key, null);
                    }
                    if (matchesQuery(rightJoinData, query)) {
                        result.append(rightJoinData).append("\n");
                    }
                }
            }
        }

        return result.length() > 0 ? result.toString() : "No matching records found.";
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
        Map<String, Database>  transaction =transactionManager.getTransaction(currentTransactionId).getTransaction();
        if (tx != null) {
            transaction.computeIfAbsent(currentDatabase, k -> new Database())
                    .getCollections().computeIfAbsent(collectionName, k -> new Collections())
                    .getDocuments().put(doc.getId(), doc);
            return "Document staged for insert in transaction";
        }
        transactionManager.setDatabase(currentTransactionId,transaction);


        collection.getDocuments().put(doc.getId(), doc);
        updateIndexes(collection, doc);
        return "Inserted document ID: " + doc.getId();
    }

    private String handleUpdate(String collectionName, String queryStr, String updateStr, Transaction tx) throws JsonProcessingException {


        Map<String, Object> query = parseJson(queryStr);
        Map<String, Object> update = parseJson(updateStr);
        int updatedCount = 0;
        if (tx != null) {
            Collections collection = getTransactionCollection(collectionName);
            if (collection == null) return "Collection not found";

            for (Document doc : collection.getDocuments().values()) {
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
            db = transactionManager.getTransaction(currentTransactionId).getTransaction().get(currentDatabase);
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
            Database db = transactionManager.getTransaction(currentTransactionId).getTransaction().get(currentDatabase);
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
