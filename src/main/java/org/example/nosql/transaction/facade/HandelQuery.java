package org.example.nosql.transaction.facade;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.example.nosql.transaction.Utils.Utils;
import org.example.nosql.transaction.exceptions.TransactionException;
import org.example.nosql.transaction.facade.operations.CrudOperations;
import org.example.nosql.transaction.structure.Collections;
import org.example.nosql.transaction.structure.Database;
import org.example.nosql.transaction.structure.Document;
import org.example.nosql.transaction.transaction.Transaction;
import org.example.nosql.transaction.transaction.TransactionManager;
import org.jetbrains.annotations.NotNull;

import java.net.Socket;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class HandelQuery implements CrudOperations {
    private static Map<String, Database> databases = new ConcurrentHashMap<>();
    private static String currentDatabase = "test";
    Utils utils=new Utils();

    final TransactionManager transactionManager = new TransactionManager();

    Long currentTransactionId = null;

    public HandelQuery( Map<String, Database> databases) {
        this.databases=databases;
    }

    public Long getCurrentTransactionId() {
        return currentTransactionId;
    }

    public void setCurrentTransactionId(Long currentTransactionId) {
        this.currentTransactionId = currentTransactionId;
    }

    public TransactionManager getTransactionManager() {
        return transactionManager;
    }

    public static Map<String, Database> getDatabases() {
        return databases;
    }

    public static void setDatabases(Map<String, Database> databases) {
        HandelQuery.databases = databases;
    }

    public static String getCurrentDatabase() {
        return currentDatabase;
    }

    public void setCurrentDatabase(String currentDatabase) {
        HandelQuery.currentDatabase = currentDatabase;
    }

    public String find(String command, String[] words, String tbl, List<String> jsonObjects) throws JsonProcessingException {
        if (words.length > 2 && words[1].equalsIgnoreCase("JOIN")) {
            return processFindJoinCommand(command);
        }
        return handleFind(tbl, jsonObjects.size() > 0 ? jsonObjects.get(0) : "{}");
    }
    @Override
    public String handleDelete(String collectionName, String queryStr, Transaction tx) throws JsonProcessingException {


        Map<String, Object> query = utils.parseJson(queryStr);
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
            Collections collection = utils.getCollection(collectionName,databases,currentDatabase);
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

    @Override
    public String handleInsert(String collectionName, String documentStr, Transaction tx) throws JsonProcessingException {
        Database db = databases.computeIfAbsent(currentDatabase, k -> new Database());
        Collections collection = db.getCollections().computeIfAbsent(collectionName, k -> new Collections());

        Map<String, Object> documentData = utils.parseJson(documentStr);
        documentData.forEach((key, value) -> {
            System.out.println("Key: " + key + ", Value: " + value);
        });
        Document doc = new Document(documentData);

        if (tx != null) {
            Map<String, Database>  transaction =transactionManager.getTransaction(currentTransactionId).getTransaction();
            transaction.computeIfAbsent(currentDatabase, k -> new Database())
                    .getCollections().computeIfAbsent(collectionName, k -> new Collections())
                    .getDocuments().put(doc.getId(), doc);
            //transactionManager.setDatabase(currentTransactionId,transaction);
            return "Document staged for insert in transaction";
        }



        collection.getDocuments().put(doc.getId(), doc);
        updateIndexes(collection, doc);
        return "Inserted document ID: " + doc.getId();
    }

    public String handleUpdate(String collectionName, String queryStr, String updateStr, Transaction tx) throws JsonProcessingException {


        Map<String, Object> query = utils.parseJson(queryStr);
        Map<String, Object> update = utils.parseJson(updateStr);
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
            Collections collection = utils.getCollection(collectionName,databases,currentDatabase);
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


    public String dropCollection(String collectionName,Transaction tx) {
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
    private String processFindJoinCommand(String command) throws JsonProcessingException {
        Pattern pattern = Pattern.compile(
                "FIND JOIN\\s+(\\w+)\\s*(\\{.*?\\})?\\s+(\\w+)\\s*(\\{.*?\\})?\\s+ON\\s+(\\w+)\\s*=\\s*(\\w+)",
                Pattern.DOTALL
        );

        Matcher matcher = pattern.matcher(command);

        if (!matcher.find()) {
            return "ERROR: Invalid JOIN syntax.";
        }

        String primaryCollection = matcher.group(1);
        String primaryFilter = matcher.group(2);
        String secondaryCollection = matcher.group(3);
        String secondaryFilter = matcher.group(4);
        String field1 = matcher.group(5);
        String field2 = matcher.group(6);

        if (primaryFilter == null) primaryFilter = "{}";
        if (secondaryFilter == null) secondaryFilter = "{}";

        System.out.println(primaryCollection+"  "+primaryFilter+"  "+secondaryCollection+"  "+secondaryFilter+"  "+field1+"  "+field2);

        return handleOptimizedFindWithJoin(
                primaryCollection,
                primaryFilter,
                secondaryCollection,
                secondaryFilter,
                field1, field2);
    }

    public String processNestedQueries(String query) throws JsonProcessingException {
        String beginning = utils.firstWord(query);
        String command = utils.removeFirstWord(query);

        StringBuilder processedQuery = new StringBuilder();

        while (true) {
            String nestedQuery = extractFindQuery(command);
            if (nestedQuery == null || nestedQuery.isEmpty()) break;

            // Avoid infinite recursion
            if (nestedQuery.equals(query)) {
                System.err.println("Warning: Infinite recursion detected! Skipping: " + nestedQuery);
                break;
            }

            // Process nested query
            String nestedAnswer = processNestedQueries(nestedQuery);

            // Execute the nested query (handleFind should return the execution result)
            String executionResult = handleFind(beginning + " " + nestedAnswer, "{}");
            System.out.println("res --->"+executionResult);

            processedQuery.append(executionResult).append(" ");

            // Replace the nested query in the original command with its result
            command = command.replace(nestedQuery, executionResult).trim();

            // Safety check to prevent infinite loops
            if (command.equals(query)) {
                System.err.println("Warning: Command is not reducing, breaking loop.");
                break;
            }
        }

        // Append any remaining unprocessed parts of the command
        if (!command.isEmpty()) {
            processedQuery.append(command);
        }

        return beginning + " " + processedQuery.toString().trim();
    }




    public static String extractFindQuery(String input) {
        String findJoinRegex = "FIND\\s+JOIN\\s+\\w+(?:\\s*\\{[^{}]*\\})?\\s+\\w+(?:\\s*\\{[^{}]*\\})?\\s+ON\\s+\\w+\\s*=\\s*\\w+";
        Pattern pattern = Pattern.compile(findJoinRegex);
        Matcher matcher = pattern.matcher(input);

        if (matcher.find()) {
            return matcher.group();
        }

        int findIndex = input.indexOf("FIND");
        if (findIndex == -1) return null;

        int braceCount = 0;
        boolean insideFind = false;
        StringBuilder result = new StringBuilder();

        for (int i = findIndex; i < input.length(); i++) {
            char c = input.charAt(i);
            result.append(c);

            if (c == '{') {
                braceCount++;
                insideFind = true;
            } else if (c == '}') {
                braceCount--;
            }

            if (insideFind && braceCount == 0) {
                break;
            }
        }

        return result.toString().trim();
    }

    private String handleOptimizedFindWithJoin(String primaryCollection, String primaryQueryStr, String secondaryCollection, String secondaryQueryStr, String field1, String field2) throws JsonProcessingException {
        Collections primary = utils.getCollection(primaryCollection,databases,currentDatabase);
        Collections secondary = utils.getCollection(secondaryCollection,databases,currentDatabase);

        if (primary == null || secondary == null) {
            return "ERROR: One or both collections not found.";
        }

        Map<String, Object> primaryQuery = primaryQueryStr.isEmpty() ? new HashMap<>() : utils.parseJson(primaryQueryStr);
        Map<String, Object> secondaryQuery = secondaryQueryStr.isEmpty() ? new HashMap<>() : utils.parseJson(secondaryQueryStr);

        List<Map<String, Object>> filteredPrimary = primary.getDocuments().values().stream()
                .map(Document::getData)
                .filter(data -> matchesQuery(data, primaryQuery))
                .collect(Collectors.toList());

        List<Map<String, Object>> filteredSecondary = secondary.getDocuments().values().stream()
                .map(Document::getData)
                .filter(data -> matchesQuery(data, secondaryQuery))
                .collect(Collectors.toList());

        StringBuilder result = new StringBuilder();

        for (Map<String, Object> doc1 : filteredPrimary) {
            Object joinValue1 = doc1.get(field1);
            boolean matched = false;

            for (Map<String, Object> doc2 : filteredSecondary) {
                Object joinValue2 = doc2.get(field2);
                System.out.println("--------->"+joinValue1+"  "+joinValue2+"   "+(joinValue2 !=null &&joinValue1 != null && joinValue1.equals(joinValue2)));
                if (joinValue2 !=null &&joinValue1 != null && joinValue1.equals(joinValue2)) {
                    Map<String, Object> mergedData = new HashMap<>(doc1);
                    mergedData.putAll(doc2);
                    result.append(mergedData).append("\n");
                    matched = true;
                }
            }

            // Handle LEFT JOIN case
            /*if (!matched) {
                Map<String, Object> leftJoinData = new HashMap<>(doc1);
                for (String key : secondary.getDocuments().values().stream().findFirst().map(Document::getData).orElse(new HashMap<>()).keySet()) {
                    leftJoinData.putIfAbsent(key, null);
                }
                result.append(leftJoinData).append("\n");
            }*/
        }

        return result.length() > 0 ? result.toString() : "No matching records found.";
    }

    private String handleFind(String collectionName, String queryStr) throws JsonProcessingException {
        Collections collection = utils.getCollection(collectionName,databases,currentDatabase);
        if (collection == null) return "Collection not found";

        Map<String, Object> query = utils.parseJson(queryStr);
        StringBuilder result = new StringBuilder();

        collection.getDocuments().values().stream()
                .filter(doc -> matchesQuery(doc.getData(), query))
                .forEach(doc -> result.append(doc.getData()).append("\n"));

        return result.length() > 0 ? result.toString() : "No documents found";
    }

    public String createIndex(String collectionName, String field,Transaction tx) {

        Collections collection =utils.getCollection(collectionName,databases,currentDatabase);
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
        collection = utils.getCollection(collectionName,databases,currentDatabase);
        if (collection == null) return "Collection not found";
        collection.getIndexes().computeIfAbsent(field, k -> ConcurrentHashMap.newKeySet());
        for (Document doc : collection.getDocuments().values()) {
            if (doc.getData().containsKey(field)) {
                collection.getIndexes().get(field).add(doc.getData().get(field).toString());
            }
        }
        return "Index created on " + field;
    }
    private void updateIndexes(Collections collection, Document doc) {
        for (String indexedField : collection.getIndexes().keySet()) {
            if (doc.getData().containsKey(indexedField)) {
                collection.getIndexes().get(indexedField).add(doc.getData().get(indexedField).toString());
            }
        }
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
                    return utils.compare(docValue, value) > 0;
                case "$lt":
                    return utils.compare(docValue, value) < 0;
                case "$in":
                    return ((Collections) value).getDocuments().contains(docValue);
                default:
                    return false;
            }
        });
    }



    private Collections getTransactionCollection(String collectionName) {
        try {
            Database db = transactionManager.getTransaction(currentTransactionId).getTransaction().get(currentDatabase);
            return (db != null) ? db.getCollections().get(collectionName) : null;
        }catch (Exception e){
            return new Collections();
        }

    }

    @NotNull
    public String RollbackTransaction() {
        if (currentTransactionId == null) return "No active transaction";
        transactionManager.rollbackTransaction(currentTransactionId);
        currentTransactionId = null;
        return "Transaction rolled back";
    }

    @NotNull
    public String CommitTransaction() throws TransactionException {
        if (currentTransactionId == null) return "No active transaction";
        transactionManager.commitTransaction(currentTransactionId,transactionManager.getDatabase(currentTransactionId));
        databases =new ConcurrentHashMap<>(transactionManager.getDatabase(currentTransactionId));
        transactionManager.removeTransaction(currentTransactionId);
        currentTransactionId = null;
        return "Transaction committed";
    }

    @NotNull
    public String beginTransaction() {
        if (currentTransactionId != null) return "Transaction already active";
        currentTransactionId = transactionManager.beginTransaction();
        transactionManager.setDatabase(currentTransactionId,databases);
        return "Transaction ID: " + currentTransactionId;
    }
}
