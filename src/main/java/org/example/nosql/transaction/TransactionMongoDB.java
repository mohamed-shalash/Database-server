package org.example.nosql.transaction;


import org.example.nosql.transaction.Utils.*;
import org.example.nosql.transaction.handler.ClientHandler;

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
    private static Utils utils=new Utils();

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

            databases.putAll(utils.loadDatabases());

            // Add shutdown hook for clean exit
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                scheduler.shutdown();
                utils.saveDatabases(databases);
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
 * add join command
 * add nested command
 * save indexes in seperate database
 * commit index
 * make find output more pretty
 * clean code
 * add view and materialized view
 * add readme file
 * use pages
 * make it in production stuff
 *
 */