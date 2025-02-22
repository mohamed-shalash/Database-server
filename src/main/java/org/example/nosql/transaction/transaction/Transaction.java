package org.example.nosql.transaction.transaction;

import lombok.Data;
import lombok.Getter;
import lombok.Setter;
import org.example.nosql.transaction.Utils.SaveData;
import org.example.nosql.transaction.structure.Database;
import org.example.nosql.transaction.Utils.Utils;
import org.example.nosql.transaction.exceptions.TransactionException;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Data
@Getter
@Setter
public class Transaction {
    private final long txId;
    private  Map<String, Database> transaction = new ConcurrentHashMap<>();
    public Transaction(long txId) {
        this.txId = txId;
    }

    public void setTransaction(Map<String, Database> databases) {
        this.transaction = new ConcurrentHashMap<>(databases);
    }

    public void commit(Map<String, Database> database) throws TransactionException {
        try {
            validate();
            applyOperations(database);
        }finally {
        }
    }

    private void validate() throws TransactionException {
        // Add validation logic for version conflicts
    }

    private void applyOperations(Map<String, Database> database) {
        SaveData saveData=new SaveData();
        saveData.saveDatabases(database);
        System.out.println("Commited successfully");
    }


}
