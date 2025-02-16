package org.example.memory;

import java.io.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.*;

public class RedisLikeDatabase {
    private static final int PORT = 12345;
    private static final ConcurrentHashMap<String, ValueEntry> database = new ConcurrentHashMap<>();

    public static void main(String[] args) {
        try (ServerSocket serverSocket = new ServerSocket(PORT)) {
            System.out.println("Redis-like server started on port " + PORT);
            new Thread(RedisLikeDatabase::cleanupExpiredKeys).start();

            while (true) {
                Socket clientSocket = serverSocket.accept();
                new Thread(new ClientHandler(clientSocket)).start();
            }
        } catch (IOException e) {
            System.err.println("Server exception: " + e.getMessage());
        }
    }

    private static void cleanupExpiredKeys() {
        while (true) {
            try {
                Thread.sleep(1000);
                long now = System.currentTimeMillis();
                database.entrySet().removeIf(entry ->
                        entry.getValue().isExpired() && entry.getValue().expiration <= now
                );
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
    }

    static class ValueEntry {
        enum Type { STRING, LIST, SET }
        Type type;
        Object data;
        long expiration = -1;

        public ValueEntry(Type type, Object data) {
            this.type = type;
            this.data = data;
        }

        public boolean isExpired() {
            return expiration != -1 && System.currentTimeMillis() > expiration;
        }

        public void setExpiration(long seconds) {
            this.expiration = System.currentTimeMillis() + (seconds * 1000);
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
                    BufferedReader in = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
                    PrintWriter out = new PrintWriter(clientSocket.getOutputStream(), true);
            ) {
                String inputLine;
                while ((inputLine = in.readLine()) != null) {
                    String[] parts = inputLine.split(" ");
                    String command = parts[0].toUpperCase();
                    String response = processCommand(command, parts);
                    out.println(response);
                }
            } catch (IOException e) {
                System.err.println("Client handler exception: " + e.getMessage());
            } finally {
                try { clientSocket.close(); } catch (IOException ignored) {}
            }
        }

        private String processCommand(String command, String[] parts) {
            try {
                switch (command) {
                    case "SET":
                        if (parts.length < 3) return "ERROR: SET requires key and value";
                        database.put(parts[1], new ValueEntry(ValueEntry.Type.STRING, parts[2]));
                        return "OK";

                    case "GET":
                        if (parts.length < 2) return "ERROR: GET requires key";
                        ValueEntry entry = database.get(parts[1]);
                        if (entry == null || entry.isExpired()) return "NIL";
                        return entry.data.toString();

                    case "EXPIRE":
                        if (parts.length < 3) return "ERROR: EXPIRE requires key and seconds";
                        entry = database.get(parts[1]);
                        if (entry == null) return "0";
                        entry.setExpiration(Long.parseLong(parts[2]));
                        return "1";

                    case "LPUSH":
                        if (parts.length < 3) return "ERROR: LPUSH requires key and value";
                        LinkedList<String> list = getOrCreateList(parts[1]);
                        list.addFirst(parts[2]);
                        return "OK";

                    case "RPUSH":
                        if (parts.length < 3) return "ERROR: RPUSH requires key and value";
                        list = getOrCreateList(parts[1]);
                        list.addLast(parts[2]);
                        return "OK";

                    case "LPOP":
                        if (parts.length < 2) return "ERROR: LPOP requires key";
                        list = getList(parts[1]);
                        return list != null && !list.isEmpty() ? list.removeFirst() : "NIL";

                    case "RPOP":
                        if (parts.length < 2) return "ERROR: RPOP requires key";
                        list = getList(parts[1]);
                        return list != null && !list.isEmpty() ? list.removeLast() : "NIL";

                    case "LLEN":
                        if (parts.length < 2) return "ERROR: LLEN requires key";
                        list = getList(parts[1]);
                        return list != null ? String.valueOf(list.size()) : "0";

                    case "SADD":
                        if (parts.length < 3) return "ERROR: SADD requires key and member";
                        Set<String> set = getOrCreateSet(parts[1]);
                        set.add(parts[2]);
                        return "OK";

                    case "SMEMBERS":
                        if (parts.length < 2) return "ERROR: SMEMBERS requires key";
                        set = getSet(parts[1]);
                        return set != null ? String.join(", ", set) : "NIL";

                    case "DEL":
                        if (parts.length < 2) return "ERROR: DEL requires key";
                        return database.remove(parts[1]) != null ? "1" : "0";

                    default:
                        return "ERROR: Unknown command";
                }
            } catch (Exception e) {
                return "ERROR: " + e.getMessage();
            }
        }

        private LinkedList<String> getOrCreateList(String key) {
            return (LinkedList<String>) 
                    database.compute(key, (k, v) -> {
                if (v == null || v.isExpired()) {
                    return new ValueEntry(ValueEntry.Type.LIST, new LinkedList<>());
                }
                if (v.type != ValueEntry.Type.LIST) throw new RuntimeException("Wrong type");
                return v;
            }).data;
        }

        private LinkedList<String> getList(String key) {
            ValueEntry entry = database.get(key);
            if (entry == null || entry.isExpired() || entry.type != ValueEntry.Type.LIST) return null;
            return (LinkedList<String>) entry.data;
        }

        private Set<String> getOrCreateSet(String key) {
            return (Set<String>) database.compute(key, (k, v) -> {
                if (v == null || v.isExpired()) {
                    return new ValueEntry(ValueEntry.Type.SET, ConcurrentHashMap.newKeySet());
                }
                if (v.type != ValueEntry.Type.SET) throw new RuntimeException("Wrong type");
                return v;
            }).data;
        }

        private Set<String> getSet(String key) {
            ValueEntry entry = database.get(key);
            if (entry == null || entry.isExpired() || entry.type != ValueEntry.Type.SET) return null;
            return (Set<String>) entry.data;
        }
    }
}