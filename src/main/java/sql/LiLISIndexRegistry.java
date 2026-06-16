package sql;

import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class LiLISIndexRegistry {
    private static final Map<String, IndexedSpatialRDD> REGISTRY = new ConcurrentHashMap<>();

    private LiLISIndexRegistry() {
    }

    public static void register(String name, IndexedSpatialRDD index) {
        if (name == null || name.trim().isEmpty()) {
            throw new IllegalArgumentException("Index name must not be empty");
        }
        if (index == null) {
            throw new IllegalArgumentException("Index must not be null");
        }
        REGISTRY.put(normalize(name), index);
    }

    public static IndexedSpatialRDD get(String name) {
        IndexedSpatialRDD index = REGISTRY.get(normalize(name));
        if (index == null) {
            throw new IllegalArgumentException("LiLIS index is not registered: " + name);
        }
        return index;
    }

    public static boolean contains(String name) {
        return REGISTRY.containsKey(normalize(name));
    }

    public static void remove(String name) {
        REGISTRY.remove(normalize(name));
    }

    public static Set<String> names() {
        return Collections.unmodifiableSet(REGISTRY.keySet());
    }

    private static String normalize(String name) {
        return name == null ? "" : name.trim().toLowerCase();
    }
}
