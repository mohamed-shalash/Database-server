package org.example.nosql.transaction.Utils;

import lombok.Getter;
import lombok.Setter;

import java.io.Serializable;
import java.util.Map;
import java.util.UUID;

@Getter
@Setter
public class Document implements Serializable {
    Map<String, Object> data;
    String id;

    public Document(Map<String, Object> data) {
        this.id = UUID.randomUUID().toString();
        this.data = data;
    }


}
