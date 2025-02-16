package org.example.memory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.*;
import java.util.concurrent.*;

public class InMemoryDatabase {
    private static final int PORT = 12345;
    private static final ConcurrentHashMap<String, String> database = new ConcurrentHashMap<>();

    public static void main(String[] args) {
        try (ServerSocket serverSocket = new ServerSocket(PORT)) {
            System.out.println("Database server started on port " + PORT);

            while (true) {
                Socket clientSocket = serverSocket.accept();
                new Thread(new ClientHandler(clientSocket)).start();
            }
        } catch (IOException e) {
            System.err.println("Server exception: " + e.getMessage());
        }
    }

    private static class ClientHandler implements Runnable {
        private final Socket clientSocket;

        public ClientHandler(Socket socket) {
            this.clientSocket = socket;
        }

        @Override
        public void run() {
            try (
                    BufferedReader in = new BufferedReader(
                            new InputStreamReader(clientSocket.getInputStream()));
                    PrintWriter out = new PrintWriter(clientSocket.getOutputStream(), true);
            ) {
                String inputLine;
                while ((inputLine = in.readLine()) != null) {
                    String[] parts = inputLine.split(" ", 3);
                    String command = parts[0].toUpperCase();
                    String response = processCommand(command, parts);
                    out.println(response);
                }
            } catch (IOException e) {
                System.err.println("Client handler exception: " + e.getMessage());
            } finally {
                try {
                    clientSocket.close();
                } catch (IOException e) {
                    System.err.println("Error closing client socket: " + e.getMessage());
                }
            }
        }

        private String processCommand(String command, String[] parts) {
            try {
                switch (command) {
                    case "SET":
                        if (parts.length < 3) return "ERROR: SET requires key and value";
                        database.put(parts[1], parts[2]);
                        return "OK";

                    case "GET":
                        if (parts.length < 2) return "ERROR: GET requires key";
                        String value = database.get(parts[1]);
                        return value != null ? value : "NOT FOUND";

                    case "DELETE":
                        if (parts.length < 2) return "ERROR: DELETE requires key";
                        database.remove(parts[1]);
                        return "DELETED";

                    case "QUIT":
                        clientSocket.close();
                        return "BYE";

                    default:
                        return "ERROR: Unknown command";
                }
            } catch (Exception e) {
                return "ERROR: " + e.getMessage();
            }
        }
    }
}
