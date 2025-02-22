package org.example.nosql.transaction.facade.operations;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.example.nosql.transaction.transaction.Transaction;

import java.util.List;

public interface CrudOperations {
    public String handleDelete(String collectionName, String queryStr, Transaction tx) throws JsonProcessingException;
    public String handleInsert(String collectionName, String documentStr, Transaction tx)
            throws JsonProcessingException;

    public String handleUpdate(String collectionName, String queryStr, String updateStr, Transaction tx) throws JsonProcessingException;

    public String find(String command, String[] words, String tbl, List<String> jsonObjects) throws JsonProcessingException;

    public String dropCollection(String collectionName,Transaction tx);

    public String createIndex(String collectionName, String field,Transaction tx);

    public String processNestedQueries(String query) throws JsonProcessingException;
}
