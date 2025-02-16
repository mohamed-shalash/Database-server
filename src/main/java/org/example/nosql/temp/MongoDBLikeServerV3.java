package org.example.nosql.temp;


import org.example.nosql.storage.PersistentMongoDB;
import org.example.nosql.transaction.Utils.Collections;
import org.example.nosql.transaction.Utils.Document;
import org.example.nosql.transaction.exceptions.TransactionException;

import java.io.Serializable;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

public class MongoDBLikeServerV3 {
   /* ConcurrentHashMap<String, Database> currentDB = new ConcurrentHashMap<>();

    public static void main(String[] args) {
        // Add this line
       TransactionManager txManager = new TransactionManager();

        // Add this line
       ClientHandler clientHandler = new ClientHandler();

        // Add this line
       Thread clientThread = new Thread(clientHandler);
        clientThread.start();
    }

 static    class TransactionManager {
        private final Map<Long, Transaction> activeTransactions = new ConcurrentHashMap<>();

        private final AtomicLong txIdCounter = new AtomicLong(0);

        public long beginTransaction() {
            long txId = txIdCounter.incrementAndGet();
            activeTransactions.put(txId, new Transaction(txId));
            return txId;
        }

        public Transaction getTransaction(long txId) {
            return activeTransactions.get(txId);
        }


        public void commitTransaction(long txId) throws TransactionException {
            Transaction tx = activeTransactions.remove(txId);
            if (tx == null) {
                throw new TransactionException("Transaction not found");
            }

            try {
                tx.commit();
            } catch (TransactionException e) {
                tx.rollback();
                throw e;
            } finally {
                tx.cleanup();
            }
        }

        public void rollbackTransaction(long txId) {
            Transaction tx = activeTransactions.remove(txId);
            if (tx != null) {
                tx.rollback();
            }
        }
    }

    static class Database implements Serializable {
        private final Map<String, Collection> collections = new ConcurrentHashMap<>();

        public Collection getCollection(String name) {
            return collections.computeIfAbsent(name, k -> new Collection());
        }
    }


    // Transaction Class


    // Modified Collection Class
    static class Collection implements Serializable {
        // Add version tracking to documents
        private final Map<String, Map<String, Object>> documents = new ConcurrentHashMap<>();
        private final Map<String, Integer> versions = new ConcurrentHashMap<>();

        public Map<String, Object> getDocumentForTransaction(String documentId, Transaction tx) {
            Map<String, Object> doc = documents.get(documentId);
            if (doc != null && tx != null) {
                tx.recordSnapshot("default", documentId, doc, versions.get(documentId));
            }
            return doc;
        }

        // Modified existing methods to use versions
        public String insert(Map<String, Object> document, Transaction tx) {
            String id = UUID.randomUUID().toString();
            if (tx != null) {
                tx.addOperation(new Transaction.Operation(
                        Transaction.OperationType.INSERT, "default", id, document));
            } else {
                documents.put(id, document);
                versions.put(id, 0);
            }
            return id;
        }

        public boolean update(String id, Map<String, Object> updates, Transaction tx) {
            if (tx != null) {
                Map<String, Object> existing = documents.get(id);
                tx.addOperation(new Transaction.Operation(
                        Transaction.OperationType.UPDATE, "default", id, updates));
                return true;
            } else {
                if (!documents.containsKey(id)) return false;
                documents.get(id).putAll(updates);
                versions.put(id, versions.get(id) + 1);
                return true;
            }
        }

        // Similar modifications for delete and other methods
    }

    // Updated ClientHandler
    private static class ClientHandler implements Runnable {
        private Long currentTransactionId = null;
        private final TransactionManager txManager = new TransactionManager();

        private String processCommand(String command, String args) {
            try {
                switch (command) {
                    case "BEGIN":
                        if (currentTransactionId != null) {
                            return "ERROR: Already in transaction";
                        }
                        currentTransactionId = txManager.beginTransaction();
                        return "TX:" + currentTransactionId;

                    case "COMMIT":
                        if (currentTransactionId == null) {
                            return "ERROR: No active transaction";
                        }
                        txManager.commitTransaction(currentTransactionId);
                        currentTransactionId = null;
                        return "Commit successful";

                    case "ROLLBACK":
                        if (currentTransactionId == null) {
                            return "ERROR: No active transaction";
                        }
                        txManager.rollbackTransaction(currentTransactionId);
                        currentTransactionId = null;
                        return "Rollback successful";

                    case "INSERT":
                        Transaction tx = currentTransactionId != null ?
                                txManager.getTransaction(currentTransactionId) : null;
                        Map<String, Object> doc = mapper.readValue(args, Map.class);
                        String id = currentDB.getCollection("default").insert(doc, tx);
                        return "Document ID: " + id;

                    case "UPDATE":
                        // Similar transaction-aware handling

                        // Other commands...
                }
            } catch (TransactionException e) {
                currentTransactionId = null;
                return "TX ERROR: " + e.getMessage();
            }
            return "nothing";
        }

        @Override
        public void run() {

        }
    }
}
/**
 * BEGIN
 * TX:123
 * INSERT {"name": "Alice", "balance": 100}
 * UPDATE account-123 {"balance": 50}
 * COMMIT
 * To Improve:
 *
 * Implement multi-version concurrency control (MVCC)
 *
 * Add proper isolation levels
 *
 * Support distributed transactions
 *
 * Add transaction logging for recovery
 *
 * Implement deadlock detection
 */

/**
 * BEGIN
 * TX:123
 * INSERT {"name": "Alice", "balance": 100}
 * UPDATE account-123 {"balance": 50}
 * COMMIT
 */
}