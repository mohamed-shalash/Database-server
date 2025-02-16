package org.example.nosql.transaction.Utils;

import lombok.Getter;
import lombok.Setter;

import java.io.Serializable;
import java.util.concurrent.ConcurrentHashMap;

@Getter
@Setter
public class Database implements Serializable {
    ConcurrentHashMap<String, Collections> collections = new ConcurrentHashMap<>();
    String name;

    public Database(String name) {
        this.name = name;
       // loadCollections();
    }
    public Database() {
    }
    public Collections getCollection(String name) {
        return collections.computeIfAbsent(name, k -> new Collections());
    }
}