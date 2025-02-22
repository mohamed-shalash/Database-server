package org.example.nosql.transaction.structure;

import lombok.Getter;
import lombok.Setter;

import java.io.Serializable;
import java.nio.file.Path;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

@Getter
@Setter
public class Collections implements Serializable {
    ConcurrentHashMap<String, Document> documents = new ConcurrentHashMap<>();
    ConcurrentHashMap<String, Set<String>> indexes = new ConcurrentHashMap<>();
    private final Map<String, Integer> versions = new ConcurrentHashMap<>();
    Path collectionPath;

    Collections(Path path) {
        this.collectionPath = path;
        //loadDocuments();
    }
    public Collections() {

    }
}
