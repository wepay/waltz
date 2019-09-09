package com.wepay.waltz.store.internal.metadata;

import com.wepay.waltz.store.exception.StoreMetadataException;

/**
 * This method implements the {@link com.wepay.zktools.zookeeper.MutexAction} for the store metadata.
 */
public interface StoreMetadataMutexAction {

    /**
     * This method applies the mutex action to the {@link StoreMetadata}'s mutex session.
     * @param session The {@link StoreMetadataMutexSession}.
     * @throws StoreMetadataException thrown if unable to read metadata from the store.
     */
    void apply(StoreMetadataMutexSession session) throws StoreMetadataException;

}
