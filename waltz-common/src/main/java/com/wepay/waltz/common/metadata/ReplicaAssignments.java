package com.wepay.waltz.common.metadata;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

/**
 * ReplicaAssignments contains storage node replica assignments metadata in zookeeper.
 */
public class ReplicaAssignments {

    private static final byte VERSION = 1;

    public final Map<String, int[]> replicas;

    /**
     * Class constructor.
     * @param replicas Mapping of Storage connect string (in host:port format) to
     *                  list of partition Ids.
     */
    public ReplicaAssignments(final Map<String, int[]> replicas) {
        this(replicas, true);
    }

    /**
     * Class constructor.
     * @param replicas Mapping of Storage connect string (in host:port format) to
     *                  list of partition Ids.
     * @param copy Boolean flag indicating whether to create a copy or not.
     */
    private ReplicaAssignments(final Map<String, int[]> replicas, boolean copy) {
        this.replicas = copy ? new HashMap<>(replicas) : replicas;
    }

    /**
     * Writes storage node replica assignment metadata via the {@link DataOutput} provided.
     * @param out The interface that converts the data to a series of bytes.
     * @throws IOException thrown if the write fails.
     */
    public void writeTo(DataOutput out) throws IOException {
        out.writeByte(VERSION);
        out.writeInt(replicas.size());
        for (Map.Entry<String, int[]> entry : replicas.entrySet()) {
            // the storage node connect string
            out.writeUTF(entry.getKey());
            // the partition ids
            int[] partitionIds = entry.getValue();
            out.writeInt(partitionIds.length);
            for (int partitionId : partitionIds) {
                out.writeInt(partitionId);
            }
        }
    }

    /**
     * Reads storage node replica assignment metadata via the {@link DataInput} provided.
     * @param in The interface that reads bytes from a binary stream and converts it
     *        to the data of required type.
     * @return Returns the storage node's {@code ReplicaAssignments}.
     * @throws IOException thrown if the read fails.
     */
    public static ReplicaAssignments readFrom(DataInput in) throws IOException {
        byte version = in.readByte();

        if (version != VERSION) {
            throw new IOException("unsupported version");
        }

        int size = in.readInt();
        Map<String, int[]> replicas = new HashMap<>();
        for (int i = 0; i < size; i++) {
            String connectString = in.readUTF();

            int[] partitionIds = new int[in.readInt()];
            for (int j = 0; j < partitionIds.length; j++) {
                partitionIds[j] = in.readInt();
            }
            replicas.put(connectString, partitionIds);
        }

        return new ReplicaAssignments(replicas, false);
    }

    @Override
    public int hashCode() {
        return replicas.hashCode();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o instanceof ReplicaAssignments) {
            ReplicaAssignments other = (ReplicaAssignments) o;
            if (!this.replicas.keySet().equals(other.replicas.keySet())) {
                return false;
            }
            for (Map.Entry<String, int[]> entry : this.replicas.entrySet()) {
                if (!Arrays.equals(entry.getValue(), other.replicas.get(entry.getKey()))) {
                    return false;
                }
            }
            return true;
        } else {
            return false;
        }
    }

}
