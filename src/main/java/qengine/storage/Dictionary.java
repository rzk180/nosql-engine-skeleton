package qengine.storage;

import java.util.HashMap;
import java.util.Map;

public class Dictionary {
    private Map<String, Integer> stringToId;
    private Map<Integer, String> idToString;
    private int nextId;

    public Dictionary() {
        this.stringToId = new HashMap<>();
        this.idToString = new HashMap<>();
        this.nextId = 1; // Les IDs commencent à 1.
    }

    public int encode(String resource) {
        if (!stringToId.containsKey(resource)) {
            stringToId.put(resource, nextId);
            idToString.put(nextId, resource);
            nextId++;
        }
        return stringToId.get(resource);
    }

    public String decode(int id) {
        return idToString.get(id);
    }
}
