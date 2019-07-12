package com.wepay.waltz.client;

public class PartitionLocalLock {

    private final String name;
    private final long id;

    public PartitionLocalLock(String name, long id) {
        if (name == null) {
            throw new NullPointerException("lock name should not be null");
        }
        this.name = name;
        this.id = id;
    }

    @Override
    public final int hashCode() {
        return name.hashCode() ^ Long.hashCode(id);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof PartitionLocalLock) {
            PartitionLocalLock other = ((PartitionLocalLock) obj);
            return this.id == other.id && this.name.equals(other.name);
        } else {
            return false;
        }
    }

}
