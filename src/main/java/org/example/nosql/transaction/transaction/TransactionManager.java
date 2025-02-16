package org.example.nosql.transaction.transaction;

import org.example.nosql.transaction.Utils.Database;
import org.example.nosql.transaction.exceptions.TransactionException;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

public class TransactionManager {
    private final ConcurrentMap<Long, Transaction> activeTransactions = new ConcurrentHashMap<>();
    private final AtomicLong txIdCounter = new AtomicLong(0);

    public long beginTransaction() {
        long txId = txIdCounter.incrementAndGet();
        activeTransactions.put(txId, new Transaction(txId));
        return txId;
    }

    public void commitTransaction(long txId, Map<String, Database> database) throws TransactionException {
        Transaction tx = activeTransactions.remove(txId);
        if (tx == null) throw new TransactionException("Invalid transaction ID");

        try {
            tx.commit(database);
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

    public Transaction getTransaction(long txId) {
        return activeTransactions.get(txId);
    }
}
