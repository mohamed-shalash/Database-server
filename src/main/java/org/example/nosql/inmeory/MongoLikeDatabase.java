package org.example.nosql.inmeory;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.example.nosql.Utils.Document;

import java.io.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.regex.*;

public class MongoLikeDatabase {
    private static final int PORT = 27017;  // Default MongoDB port
    private static final ConcurrentHashMap<String, Database> databases = new ConcurrentHashMap<>();

    public static void main(String[] args) {
        try (ServerSocket serverSocket = new ServerSocket(PORT)) {
            System.out.println("Mongo-like server started on port " + PORT);

            while (true) {
                Socket clientSocket = serverSocket.accept();
                new Thread(new ClientHandler(clientSocket)).start();
            }
        } catch (IOException e) {
            System.err.println("Server exception: " + e.getMessage());
        }
    }

    static class Database {
        ConcurrentHashMap<String, Collection> collections = new ConcurrentHashMap<>();
    }

    static class Collection {
        ConcurrentHashMap<String, Document> documents = new ConcurrentHashMap<>();
        ConcurrentHashMap<String, Set<String>> indexes = new ConcurrentHashMap<>();
    }



    private static class ClientHandler implements Runnable {
        private final Socket clientSocket;
        private String currentDatabase = "test";

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

        private String processCommand(String command) {
            try {

                String[] words = command.split("\\s+", 3);
                String operation = words[0].toUpperCase(); // "insert" or "update"
                String tbl = words[1]; // "tbl"
                List<String> jsonObjects = extractJsonObjects(command);new ArrayList<>();
                /*Pattern jsonPattern = Pattern.compile("\\{.*?\\}");
                Matcher matcher = jsonPattern.matcher(command);

                while (matcher.find()) {
                    jsonObjects.add(matcher.group());
                }*///insert x { "name":"ali" ,age:24 ,hobbies :["eat","code"]}

                //String operation = parts[0].toUpperCase();
                switch (operation) {
                    case "USE":
                        currentDatabase = tbl;
                        return "Switched to database " + currentDatabase;

                    case "INSERT":
                        return handleInsert(tbl, jsonObjects.get(0));

                    case "FIND":
                        return handleFind(tbl, jsonObjects.size() > 0 ? jsonObjects.get(0) : "{}");

                    case "UPDATE":
                        return handleUpdate(tbl,jsonObjects.get(0), jsonObjects.get(1));

                    case "DELETE":
                        return handleDelete(tbl, jsonObjects.get(0));

                    case "CREATEINDEX":
                        return createIndex(tbl, words[2]);

                    case "DROP":
                        return dropCollection(tbl);

                    default:
                        return "ERROR: Unknown command";
                }
            } catch (Exception e) {
                return "ERROR: " + e.getMessage();
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

        private String handleInsert(String collectionName, String documentStr) throws JsonProcessingException {
            Database db = databases.computeIfAbsent(currentDatabase, k -> new Database());
            Collection collection = db.collections.computeIfAbsent(collectionName, k -> new Collection());

            System.out.println(documentStr.toString());
            System.out.println(collectionName);
            Map<String, Object> documentData = parseJson(documentStr);//parseDocument(documentStr);
            documentData.forEach((key, value) -> {
                System.out.println("Key: " + key + ", Value: " + value);
            });
            Document doc = new Document(documentData);
            collection.documents.put(doc.getId(), doc);
            updateIndexes(collection, doc);
            return "Inserted document ID: " + doc.getId();
        }

        private String handleFind(String collectionName, String queryStr) throws JsonProcessingException {
            Collection collection = getCollection(collectionName);
            if (collection == null) return "Collection not found";

            Map<String, Object> query = parseJson(queryStr);
            StringBuilder result = new StringBuilder();

            collection.documents.values().stream()
                    .filter(doc -> matchesQuery(doc.getData(), query))
                    .forEach(doc -> result.append(doc.getData()).append("\n"));

            return result.length() > 0 ? result.toString() : "No documents found";
        }



        private String handleUpdate(String collectionName, String queryStr, String updateStr) throws JsonProcessingException {
            Collection collection = getCollection(collectionName);
            if (collection == null) return "Collection not found";

            Map<String, Object> query = parseJson(queryStr);//parseDocument(queryStr);
            Map<String, Object> update = parseJson(updateStr);

            int updatedCount = 0;
            for (Document doc : collection.documents.values()) {
                if (matchesQuery(doc.getData(), query)) {
                    doc.getData().putAll(update);
                    updatedCount++;
                }
            }
            return "Updated " + updatedCount + " documents";
        }

        private String handleDelete(String collectionName, String queryStr) throws JsonProcessingException {
            Collection collection = getCollection(collectionName);
            if (collection == null) return "Collection not found";

            Map<String, Object> query = parseJson(queryStr);
            int deletedCount = 0;

            Iterator<Map.Entry<String, Document>> it = collection.documents.entrySet().iterator();
            while (it.hasNext()) {
                Document doc = it.next().getValue();
                if (matchesQuery(doc.getData(), query)) {
                    it.remove();
                    deletedCount++;
                }
            }
            return "Deleted " + deletedCount + " documents";
        }

        private String createIndex(String collectionName, String field) {
            Collection collection = getCollection(collectionName);
            if (collection == null) return "Collection not found";

            collection.indexes.computeIfAbsent(field, k -> ConcurrentHashMap.newKeySet());
            for (Document doc : collection.documents.values()) {
                if (doc.getData().containsKey(field)) {
                    collection.indexes.get(field).add(doc.getData().get(field).toString());
                }
            }
            return "Index created on " + field;
        }

        private String dropCollection(String collectionName) {
            Database db = databases.get(currentDatabase);
            if (db == null) return "Database not found";
            db.collections.remove(collectionName);
            return "Collection dropped";
        }

       /* private boolean matchesQuery(Map<String, Object> document, Map<String, Object> query) {
            return query.entrySet().stream()
                    .allMatch(entry -> document.containsKey(entry.getKey()) &&
                            document.get(entry.getKey()).equals(entry.getValue()));
        }*/
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
                        return ((Collection) value).documents.contains(docValue);
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
        private Collection getCollection(String collectionName) {
            Database db = databases.get(currentDatabase);
            return (db != null) ? db.collections.get(collectionName) : null;
        }

        private Map<String, Object> parseDocument(String documentStr) {
            Map<String, Object> doc = new HashMap<>();
            Matcher matcher = Pattern.compile("(\\w+):\\s*([^,]+)").matcher(documentStr);
            while (matcher.find()) {
                String key = matcher.group(1);
                String value = matcher.group(2).trim();
                doc.put(key, tryParseNumber(value));
            }
            return doc;
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

        private Object tryParseNumber(String value) {
            try {
                return Integer.parseInt(value);
            } catch (NumberFormatException e) {
                try {
                    return Double.parseDouble(value);
                } catch (NumberFormatException e2) {
                    return value.replaceAll("^[\"']|[\"']$", "");
                }
            }
        }

        private void updateIndexes(Collection collection, Document doc) {
            for (String indexedField : collection.indexes.keySet()) {
                if (doc.getData().containsKey(indexedField)) {
                    collection.indexes.get(indexedField).add(doc.getData().get(indexedField).toString());
                }
            }
        }
    }
}

/**
 * USE mydb
 * INSERT users {name: "Alice", age: 30, email: "alice@example.com"}
 * INSERT users {name: "Bob", age: 25, email: "bob@example.com"}
 * FIND users {age: 30}
 * UPDATE users {name: "Alice"} {age: 31}
 * CREATEINDEX users email
 * DELETE users {age: 25}
 */
