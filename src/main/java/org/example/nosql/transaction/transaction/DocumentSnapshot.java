package org.example.nosql.transaction.transaction;

import java.util.HashMap;
import java.util.Map;

public class DocumentSnapshot {
    private final Map<String, Object> data;
    private final int version;

    public DocumentSnapshot(Map<String, Object> data, int version) {
        this.data = data;
        this.version = version;
    }

    public Map<String, Object> getData() {
        return new HashMap<>(data);
    }

    public int getVersion() {
        return version;
    }
}
