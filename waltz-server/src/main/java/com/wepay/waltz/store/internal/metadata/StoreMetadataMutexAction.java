package com.wepay.waltz.store.internal.metadata;

import com.wepay.waltz.store.exception.StoreMetadataException;

public interface StoreMetadataMutexAction {

    void apply(StoreMetadataMutexSession session) throws StoreMetadataException;

}
