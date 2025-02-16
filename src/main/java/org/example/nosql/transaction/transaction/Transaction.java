package org.example.nosql.transaction.transaction;

import org.example.nosql.transaction.Utils.Database;
import org.example.nosql.transaction.Utils.Utils;
import org.example.nosql.transaction.exceptions.TransactionException;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class Transaction {
    private final long txId;
    private final Map<String, Map<String, DocumentSnapshot>> snapshots = new ConcurrentHashMap<>();
    private final Map<String, List<Operation>> operations = new ConcurrentHashMap<>();
    private boolean active = true;

    public enum OperationType { INSERT, UPDATE, DELETE }

    public static class Operation {
        public final OperationType type;
        public final String collection;
        public final String documentId;
        public final Map<String, Object> data;

        public Operation(OperationType type, String collection, String documentId, Map<String, Object> data) {
            this.type = type;
            this.collection = collection;
            this.documentId = documentId;
            this.data = data;
        }
    }

    public Transaction(long txId) {
        this.txId = txId;
    }

    public void addOperation(Operation operation) {
        if (active) {
            operations.computeIfAbsent(operation.collection, k -> new ArrayList<>()).add(operation);
        }
    }

    public void commit(Map<String, Database> database) throws TransactionException {
        try {
            validate();
            applyOperations(database);
        } finally {
            cleanup();
        }
    }

    private void validate() throws TransactionException {
        // Add validation logic for version conflicts
    }

    private void applyOperations(Map<String, Database> database) {
        Utils utils=new Utils();
        utils.saveDatabases(database);
        System.out.println("Commited successfully");
    }

    public void rollback() {
        cleanup();
    }

    public void cleanup() {
        operations.clear();
        snapshots.clear();
        active = false;
    }

    public boolean isActive() {
        return active;
    }
}
