package com.wepay.waltz.common.util;

import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class LRUCacheTest {
    private static final int MAX_CAPACITY = 5;
    private List<Integer> evictedEntryList = new ArrayList<>();

    @Test
    public void testLruCache() {
        LRUCache<Integer, Object> lruCache = new LRUCache<>(MAX_CAPACITY, entry -> {
            evictedEntryList.add(((Map.Entry<Integer, Object>) entry).getKey());
        });

        // Add 10 entries to LRU Cache
        for (int i = 0; i < 10; i++) {
            lruCache.putIfAbsent(i, i);
        }

        // Verify size of the Cache
        assertEquals(lruCache.size(), MAX_CAPACITY);

        // Verify cleanupFunc is called. Note: Evicted entries will be added to the evictedEntryList when cleanupFunc is called
        assertEquals(evictedEntryList.size(), 5);

        // Verify first 5 entries are not in the LRU Cache and also verify cleanup function
        for (int i = 0; i < 5; i++) {
            assertNull(lruCache.get(i));
            assertNotNull(evictedEntryList.get(evictedEntryList.indexOf(i)));
        }

        // Verify last 5 entries are in the LRU Cache
        for (int i = 5; i < 10; i++) {
            assertNotNull(lruCache.get(i));
        }

        // Access entries in the LRU Cache in a specific order
        List<Integer> list = new ArrayList<>(Arrays.asList(6, 5, 8, 7, 9));
        for (Integer idx : list) {
            assertNotNull(lruCache.get(idx));
        }

        // Verify access order
        assertEquals(list, new ArrayList<>(lruCache.keySet()));

        // Delete few entries
        lruCache.remove(5);
        lruCache.remove(6);

        // Verify deleted entries
        assertNull(lruCache.get(5));
        assertNull(lruCache.get(6));

        // Verify access order after delete
        int idx = list.indexOf(5);
        list.remove(idx);
        idx = list.indexOf(6);
        list.remove(idx);
        assertEquals(list, new ArrayList<>(lruCache.keySet()));

        // Remove all entries
        lruCache.remove(7);
        lruCache.remove(8);
        lruCache.remove(9);

        // Verify cache is empty
        assertTrue(lruCache.isEmpty());
    }

}
