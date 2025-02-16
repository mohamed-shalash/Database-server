package org.example.nosql.Utils;

import lombok.Getter;
import lombok.Setter;

import java.nio.file.Path;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;

@Getter
@Setter
class Collection {
    ConcurrentHashMap<String, Document> documents = new ConcurrentHashMap<>();
    ConcurrentSkipListMap<String, String> index = new ConcurrentSkipListMap<>(); // Simple B-tree-like index
    Path collectionPath;

    Collection(Path path) {
        this.collectionPath = path;
        //loadDocuments();
    }
    Collection() {

    }
}
