package com.wepay.waltz.client;

/**
 * A local representation of a lock sent along with the transaction payload to Waltz cluster.
 */
public class PartitionLocalLock {

    private final String name;
    private final long id;

    /**
     * Class Constructor.
     *
     * @param name the lock name.
     * @param id the lock id.
     * @throws NullPointerException if the lock name is null.
     */
    public PartitionLocalLock(String name, long id) {
        if (name == null) {
            throw new NullPointerException("lock name should not be null");
        }
        this.name = name;
        this.id = id;
    }

    /**
     * Implements {@link Object#hashCode()} as,
     * <pre>
     * {@code return name.hashCode() ^ Long.hashCode(id)}
     * </pre>
     *
     * @return hashcode.
     */
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

    public String getName() {
        return name;
    }

    public long getId() {
        return id;
    }
}
