package com.wepay.waltz.store.internal.metadata;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class PartitionMetadata {

    public static final PartitionMetadata EMPTY = new PartitionMetadata(-1, -1L, Collections.emptyMap());

    private static final byte VERSION = 1;
    private static final long DIVISOR = 0xFFFFFFFFL;

    public final int generation;
    public final long sessionId;
    public final Map<ReplicaId, ReplicaState> replicaStates;

    public PartitionMetadata(final int generation, final long sessionId, Map<ReplicaId, ReplicaState> replicaStates) {
        this(generation, sessionId, replicaStates, true);
    }

    private PartitionMetadata(final int generation, final long sessionId, Map<ReplicaId, ReplicaState> replicaStates, boolean copy) {
        this.generation = generation;
        this.sessionId = sessionId;
        this.replicaStates = copy ? new HashMap<>(replicaStates) : replicaStates;
    }

    public void writeTo(DataOutput out) throws IOException {
        out.writeByte(VERSION);
        out.writeInt(generation);
        out.writeLong(sessionId);
        out.writeInt(replicaStates.size());
        for (ReplicaState replicaState : replicaStates.values()) {
            replicaState.writeTo(out);
        }
    }

    public static PartitionMetadata readFrom(DataInput in) throws IOException {
        byte version = in.readByte();

        if (version != VERSION) {
            throw new IOException("unsupported version");
        }

        int generation = in.readInt();
        long sessionId = in.readLong();
        int numReplicaStates = in.readInt();
        Map<ReplicaId, ReplicaState> replicaStates = new HashMap<>();
        for (int i = 0; i < numReplicaStates; i++) {
            ReplicaState replicaState = ReplicaState.readFrom(in);
            replicaStates.put(replicaState.replicaId, replicaState);
        }

        return new PartitionMetadata(generation, sessionId, replicaStates, false);
    }

    @Override
    public int hashCode() {
        return (int) ((generation ^ sessionId) /  DIVISOR) ^ replicaStates.hashCode();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o instanceof PartitionMetadata) {
            PartitionMetadata other = (PartitionMetadata) o;
            return (this.generation == other.generation
                && this.sessionId == other.sessionId
                && this.replicaStates.equals(other.replicaStates)
            );
        } else {
            return false;
        }
    }

}
