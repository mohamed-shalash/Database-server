package org.example.nosql.transaction.transaction;

import org.example.nosql.transaction.structure.Database;
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
        Transaction tx = activeTransactions.get(txId);
        if (tx == null) throw new TransactionException("Invalid transaction ID");
        tx.commit(database);
    }

    public void rollbackTransaction(long txId) {
        removeTransaction(txId);
    }

    public Transaction getTransaction(long txId) {
        return activeTransactions.get(txId);
    }

    public void setDatabase(long txId, Map<String, Database> database){
        Transaction tx = activeTransactions.get(txId);
        if (tx != null) {
            tx.setTransaction(database);
        }
    }

    public Map<String, Database> getDatabase(long txId){
        return activeTransactions.get(txId).getTransaction();
    }

    public void removeTransaction(long txId){
        activeTransactions.remove(txId);
    }

}
