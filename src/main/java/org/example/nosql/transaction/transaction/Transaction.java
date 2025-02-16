package org.example.nosql.transaction.transaction;

import lombok.Data;
import lombok.Getter;
import lombok.Setter;
import org.example.nosql.transaction.Utils.Database;
import org.example.nosql.transaction.Utils.Utils;
import org.example.nosql.transaction.exceptions.TransactionException;

import java.util.ArrayList;
import java.util.List;
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
        Utils utils=new Utils();
        utils.saveDatabases(database);
        System.out.println("Commited successfully");
    }


}
