package com.wepay.waltz.store.internal.metadata;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class ReplicaId implements Comparable<ReplicaId> {

    private static final byte VERSION = 1;

    public final int partitionId;
    public final String storageNodeConnectString;

    public ReplicaId(int partitionId, String storageNodeConnectString) {
        this.partitionId = partitionId;
        this.storageNodeConnectString = storageNodeConnectString;
    }

    public void writeTo(DataOutput out) throws IOException {
        out.writeByte(VERSION);
        out.writeInt(partitionId);
        out.writeUTF(storageNodeConnectString);
    }

    public static ReplicaId readFrom(DataInput in) throws IOException {
        byte version = in.readByte();

        if (version != VERSION) {
            throw new IOException("unsupported version");
        }

        int partitionId = in.readInt();
        String storageNodeConnectString = in.readUTF();

        return new ReplicaId(partitionId, storageNodeConnectString);
    }


    @Override
    public int hashCode() {
        return partitionId * storageNodeConnectString.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        return obj instanceof ReplicaId
            && this.partitionId == ((ReplicaId) obj).partitionId
            && this.storageNodeConnectString.equals(((ReplicaId) obj).storageNodeConnectString);
    }

    @Override
    public int compareTo(ReplicaId other) {
        int cmp = Integer.compare(partitionId, other.partitionId);
        if (cmp != 0) {
            return cmp;
        }
        return storageNodeConnectString.compareTo(other.storageNodeConnectString);
    }

    @Override
    public String toString() {
        return "ReplicaId(" + partitionId + "," + storageNodeConnectString + ")";
    }

}
