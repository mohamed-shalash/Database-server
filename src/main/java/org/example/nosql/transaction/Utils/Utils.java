package org.example.nosql.transaction.Utils;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.example.nosql.transaction.structure.Collections;
import org.example.nosql.transaction.structure.Database;

import java.io.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class Utils {
    public List<String> extractJsonObjects(String command) {
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

    public static String removeFirstWord(String str) {
        String[] words = str.split(" ", 2);
        return (words.length > 1) ? words[1] : "";
    }

    public static String firstWord(String str) {
        return str.split(" ", 2)[0];
    }

    public int compare(Object a, Object b) {
        if (a instanceof Comparable && b instanceof Comparable) {
            return ((Comparable) a).compareTo(b);
        }
        return 0;
    }

    public Map<String,Object> parseJson(String input) throws JsonProcessingException {
        System.out.println(input);
        String json = input
                .replaceAll("([a-zA-Z0-9_]+)\\s*:", "\"$1\":") // Quote keys
                .replaceAll(":\\s*([a-zA-Z_]+)(?=[,}])", ":\"$1\""); // Quote only non-number values

        System.out.println("Normalized JSON: " + json);

        ObjectMapper objectMapper = new ObjectMapper();
        return objectMapper.readValue(json, Map.class);
    }

    public org.example.nosql.transaction.structure.Collections getCollection(String collectionName,Map<String, Database> databases,String currentDatabase) {
        try {
            Database db = databases.get(currentDatabase);
            return (db != null) ? db.getCollections().get(collectionName) : null;
        }catch (Exception e){
            return new Collections();
        }

    }
}
