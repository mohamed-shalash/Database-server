package org.example.nosql.temp;
import com.fasterxml.jackson.core.JsonProcessingException;
import java.util.regex.*;

public class MongoDBLikeServerV3 {
    public static void main(String[] args) throws JsonProcessingException {
        String[] queries = {
                "FIND orders",
                "FIND users { \"age\": { \"$gt\": 30 } }",
                "FIND transactions { \"amount\": 100, \"status\": \"pending\" }",
                "FIND orders {\"orderTotal\": {\"$gt\": 500}}",
                "FIND customers {\"customerId\": {\"$in\": FIND orders {\"orderTotal\": {\"$gt\": 500}}}}",
                "xyz FIND JOIN z { \"product\": \"Laptop\" } y { \"productName\": \"Laptop\" } ON product = productName",
                "FIND JOIN z { \"product\": \"Laptop\" } y  ON product = productName",
                "FIND JOIN z  y { \"productName\": \"Laptop\" } ON product = productName",
                "FIND JOIN z  y { \"productName\": {FIND JOIN z  y { \"productName\": \"Laptop\" } ON product = productName} } ON product = productName"
        };

        for (String query : queries) {
            System.out.println("Query: " + query);
            String result = processNestedQueries(query);
            System.out.println("Extracted: " + result);
            System.out.println("---------------------------------");
        }
    }

    private static String processNestedQueries(String query) {
        String beginning = firstWord(query);
        String command = removeFirstWord(query);

        // Track processed parts of the query
        StringBuilder processedQuery = new StringBuilder();

        while (true) {
            String nestedQuery = extractFindQuery(command);
            if (nestedQuery == null || nestedQuery.isEmpty()) break;

            // Process nested queries
            String smaller = processNestedQueries(nestedQuery);
            processedQuery.append(smaller).append(" ");

            // Remove processed nested query
            command = command.replace(nestedQuery, "").trim();
        }

        return beginning + " " + processedQuery.toString().trim();
    }

    public static String removeFirstWord(String str) {
        String[] words = str.split(" ", 2);
        return (words.length > 1) ? words[1] : "";
    }

    public static String firstWord(String str) {
        return str.split(" ", 2)[0];
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
}

    /*    public static String extractLargestFind(String query) {
            // Regular expression pattern to match FIND and FIND JOIN statements
            String regex = "FIND JOIN\\s+(\\w+)\\s*(\\{.*?\\})?\\s+(\\w+)\\s*(\\{.*?\\})?\\s+ON\\s+(\\w+)\\s*=\\s*(\\w+)";
            regex = "FIND\\s+\\w+(?:\\s*\\{(?:[^{}]*|\\{(?:[^{}]*|\\{[^{}]*\\})*\\})*\\})?";

            Pattern pattern = Pattern.compile(regex, Pattern.DOTALL);
            Matcher matcher = pattern.matcher(query);

            String largestFind = null;

            // Iterate over all matches
            while (matcher.find()) {
                String match = matcher.group();
                if (largestFind == null || match.length() > largestFind.length()) {
                    largestFind = match;
                }
            }

            return largestFind;
        }

        /*
    public static String extractFindJoinQuery(String input) {
        String regex = "FIND\\s+JOIN\\s+\\w+(?:\\s*\\{.*?\\})?\\s+\\w+(?:\\s*\\{.*?\\})?\\s+ON\\s+.*";

        Pattern pattern = Pattern.compile(regex, Pattern.DOTALL);
        Matcher matcher = pattern.matcher(input);

        if (matcher.find()) {
            return matcher.group();
        } else {
            return "No FIND JOIN query found!";
        }
    }
*/
    //      String query1 = " IND customers {\"customerId\": {\"$in\": FIND orders {\"orderTotal\": {\"$gt\": 500}}}}";
    //      String query2 = "customers {\"customerId\": {\"$in\": FIND JOIN orders{\"orderTotal\": {\"data\": 500}} customers {\"orderTotal\": {\"$gt\": 500}} ON customerId = customerId  }}";
    //       String query3 = "customers {\"customerId\": {\"$in\": FIND JOIN orders customers {\"orderTotal\": {\"$gt\": 500}} ON customerId = customerId  }}";
     /* public static String extractFindQuery(String input) {
          int findIndex = input.indexOf("FIND");
          if (findIndex == -1) return null; // No FIND found

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

              // If we've captured a full `FIND` statement, stop
              if (insideFind && braceCount == 0) {
                  break;
              }
          }

          return result.toString();
      }*/



