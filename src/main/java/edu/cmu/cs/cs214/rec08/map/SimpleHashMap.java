package edu.cmu.cs.cs214.rec08.map;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import net.jcip.annotations.ThreadSafe;

/**
 * Thread-safe implementation of SimpleHashMap with fine-grained locking.
 *
 * @param <K> Key type
 * @param <V> Value type
 */
@ThreadSafe
public class SimpleHashMap<K, V> {
    private final List<List<Entry<K, V>>> table;
    private final int numBuckets;

    // Array of lock objects, one per bucket
    private final Object[] locks;

    /**
     * Constructs a new hash map with a given number of buckets.
     */
    public SimpleHashMap(int numBuckets) {
        if (numBuckets <= 0) {
            throw new IllegalArgumentException("Illegal number of buckets: " + numBuckets);
        }

        this.numBuckets = numBuckets;
        table = new ArrayList<>(this.numBuckets);
        locks = new Object[this.numBuckets]; // Initialize lock array

        for (int i = 0; i < numBuckets; i++) {
            table.add(new LinkedList<>());
            locks[i] = new Object(); // Create a lock for each bucket
        }
    }

    /**
     * Puts a new key-value pair into the map.
     *
     * @param key   The key to add to the map.
     * @param value The value to add to the map for the key.
     * @return The previous value for the given key, or null if the given key was not previously in the map.
     */
    public V put(K key, V value) {
        if (key == null)
            throw new NullPointerException("Key can't be null.");

        int bucketIndex = hash(key);
        List<Entry<K, V>> bucket = table.get(bucketIndex);

        // Synchronize on the lock for the specific bucket
        synchronized (locks[bucketIndex]) {
            for (Entry<K, V> e : bucket) {
                if (e.key.equals(key)) {
                    V oldValue = e.value;
                    e.value = value;
                    return oldValue;
                }
            }
            bucket.add(new Entry<>(key, value));
        }
        return null;
    }

    /**
     * Returns the value for the given key, or null if the key is not present.
     *
     * @param key The key for which to return the value.
     * @return The value for the given key, or null if the key is not present.
     */
    public V get(K key) {
        int bucketIndex = hash(key);
        List<Entry<K, V>> bucket = table.get(bucketIndex);

        // Synchronize on the lock for the specific bucket
        synchronized (locks[bucketIndex]) {
            for (Entry<K, V> e : bucket) {
                if (e.key.equals(key)) {
                    return e.value;
                }
            }
        }
        return null;
    }

    /**
     * Returns a hash code for an object, bound to the number of buckets in the hash table.
     *
     * @param o The object to hash.
     * @return The hash code for o, bound to the number of buckets in the table.
     */
    private int hash(Object o) {
        if (o == null)
            return 0;
        return Math.abs(o.hashCode() % numBuckets);
    }

    /**
     * Entry class to store key-value pairs in the hash map.
     */
    private static class Entry<K, V> {
        final K key;
        V value;

        Entry(K key, V value) {
            this.key = key;
            this.value = value;
        }
    }
}
