package com.wepay.waltz.common.util;

import net.jcip.annotations.NotThreadSafe;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.Consumer;

/**
 * Maintains most recently accessed entries (number of entries are limited by @param maxCapacity). The order of the entries is same
 * as the order in which they are last accessed (i.e. least-recently accessed to most-recent). The initial capacity would be same as
 * the max capacity provided.
 * cleanupFunc is not called when remove(K key) method is called. It's the callers responsibility to cleanup the removed object.
 *
 */
@NotThreadSafe
public class LRUCache<K, V> extends LinkedHashMap<K, V> {

    private static final float DEFAULT_LOAD_FACTOR = 0.75f;

    private final int maxCapacity;
    protected Consumer<Map.Entry<K, V>> cleanupFunc;

    public LRUCache(int maxCapacity, Consumer<Map.Entry<K, V>> cleanupFunc) {
        super(maxCapacity, DEFAULT_LOAD_FACTOR, true);
        this.maxCapacity = maxCapacity;
        this.cleanupFunc = cleanupFunc;
    }

    public LRUCache(int maxCapacity, float loadFactor, Consumer<Map.Entry<K, V>> cleanupFunc) {
        super(maxCapacity, loadFactor, true);
        this.maxCapacity = maxCapacity;
        this.cleanupFunc = cleanupFunc;
    }

    @Override
    protected boolean removeEldestEntry(Map.Entry<K, V> eldest) {
        if (this.size() > maxCapacity) {
            if (cleanupFunc != null) {
                this.cleanupFunc.accept(eldest);
            }
            return true;
        }
        return false;
    }

}
