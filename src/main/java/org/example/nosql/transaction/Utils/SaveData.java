package org.example.nosql.transaction.Utils;

import org.example.nosql.transaction.structure.Database;

import java.io.*;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class SaveData {
    private static final String SAVE_FILE = "database.ser";
    public static synchronized  void saveDatabases(Map<String, Database> databases) {
        try (ObjectOutputStream oos = new ObjectOutputStream(new FileOutputStream(SAVE_FILE))) {
            oos.writeObject(new ConcurrentHashMap<>(databases));
            System.out.println("Database snapshot saved");
        } catch (IOException e) {
            System.err.println("Failed to save databases: " + e.getMessage());
        }
    }

    @SuppressWarnings("unchecked")
    public static synchronized ConcurrentHashMap<String, Database> loadDatabases() {
        File file = new File(SAVE_FILE);
        ConcurrentHashMap<String, Database> loaded=new ConcurrentHashMap<>();
        if (file.exists()) {
            try (ObjectInputStream ois = new ObjectInputStream(new FileInputStream(file))) {
                loaded = (ConcurrentHashMap<String, Database>) ois.readObject();
                //databases.putAll(loaded);
                System.out.println("Loaded databases from disk");
            } catch (IOException | ClassNotFoundException e) {
                System.err.println("Failed to load databases: " + e.getMessage());
            }
        }
        return loaded;
    }
}
