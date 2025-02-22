package org.example.nosql.transaction.handler;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.example.nosql.transaction.Utils.Utils;
import org.example.nosql.transaction.facade.HandelQuery;
import org.example.nosql.transaction.structure.Database;
import org.example.nosql.transaction.structure.Document;
import org.example.nosql.transaction.exceptions.TransactionException;
import org.example.nosql.transaction.transaction.Transaction;
import org.example.nosql.transaction.transaction.TransactionManager;
import org.jetbrains.annotations.NotNull;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class ClientHandler implements Runnable  {
    private final Socket clientSocket;
    private static Map<String, Database> databases = new ConcurrentHashMap<>();

    public ClientHandler(Socket socket, Map<String, Database> databases) {
        this.databases=databases;
        this.clientSocket = socket;
        crudOperations= new HandelQuery(databases);
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

    private final HandelQuery crudOperations ;
    Utils utils=new Utils();

    private String processCommand(String command) throws JsonProcessingException {
        try {


            switch (command.toUpperCase()) {

                case "BEGIN":
                    return crudOperations.beginTransaction();

                case "COMMIT":
                    return crudOperations.CommitTransaction();

                case "ROLLBACK":
                    return crudOperations.RollbackTransaction();
            }

            Transaction transaction = crudOperations.getCurrentTransactionId() != null ?
                    crudOperations.getTransactionManager().getTransaction(crudOperations.getCurrentTransactionId()) : null;

            command = crudOperations.processNestedQueries(command);

            String[] words = command.split("\\s+",3);
            String operation = words[0].toUpperCase(); // "insert" or "update"
            String tbl = words.length>1?words[1]:""; // "tbl"
            List<String> jsonObjects = utils.extractJsonObjects(command);new ArrayList<>();

            // Handle other commands with transaction context
            switch (operation) {
                case "USE":
                    crudOperations.setCurrentDatabase(words[1]);
                    return "Switched to database " + crudOperations.getCurrentDatabase();

                case "INSERT":
                    return crudOperations.handleInsert(tbl, jsonObjects.get(0), transaction);
                case "FIND":
                    return crudOperations.find(command, words, tbl, jsonObjects);

                case "UPDATE":
                    jsonObjects = utils.extractJsonObjects(command);
                    return crudOperations.handleUpdate(words[1], jsonObjects.get(0), jsonObjects.get(1), transaction);
                case "DELETE":
                    return crudOperations.handleDelete(tbl, jsonObjects.get(0),transaction);
                case "CREATE_INDEX":
                    return crudOperations.createIndex(tbl, words[2],transaction);
                case "DROP":
                    return crudOperations.dropCollection(tbl,transaction);
                // Modify other command handlers similarly

                default:
                    return "ERROR: Unknown command";
            }
        } catch (TransactionException e) {
            crudOperations.setCurrentTransactionId(null);
            return "TX ERROR: " + e.getMessage();
        }catch (Exception e){
            System.out.println(e);
            return "unValid command";
        }
    }




}
