package org.example.nosql.Utils;

import lombok.Getter;
import lombok.Setter;
import org.example.nosql.transaction.TransactionMongoDB;

import java.io.Serializable;
import java.util.concurrent.ConcurrentHashMap;

@Getter
@Setter
public class Database implements Serializable {
    ConcurrentHashMap<String, Collection> collections = new ConcurrentHashMap<>();
    String name;

    Database(String name) {
        this.name = name;
       // loadCollections();
    }
    public Collection getCollection(String name) {
        return collections.computeIfAbsent(name, k -> new Collection());
    }
}