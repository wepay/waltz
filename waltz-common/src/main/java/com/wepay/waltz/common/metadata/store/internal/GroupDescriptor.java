package com.wepay.waltz.common.metadata.store.internal;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * GroupDescriptor contains storage node group metadata in Zookeeper. With current
 * data structure, each storage node can only be assigned to one group. It can be
 * persist to and consume from Zookeeper with {@link GroupDescriptorSerializer},
 */
public class GroupDescriptor {

    private static final byte VERSION = 1;

    public final Map<String, Integer> groups; // <storage_node_connect_string, group_id>

    /**
     * Initialize GroupDescriptor with a copy of groups.
     * @param groups
     */
    public GroupDescriptor(final Map<String, Integer> groups) {
        this(groups, true);
    }

    /**
     * Initialize GroupDescriptor with groups or with a copy of groups.
     * @param groups
     * @param copy
     */
    private GroupDescriptor(final Map<String, Integer> groups, boolean copy) {
        this.groups = copy ? new HashMap<>(groups) : groups;
    }

    /**
     * Writes storage node group metadata via the {@link DataOutput} provided.
     * @param out The interface that converts the data to a series of bytes.
     * @throws IOException thrown if the write fails.
     */
    public void writeTo(DataOutput out) throws IOException {
        out.writeByte(VERSION);
        out.writeInt(groups.size());
        for (Map.Entry<String, Integer> entry: groups.entrySet()) {
            // the storage node connect string
            out.writeUTF(entry.getKey());
            // the groupId
            out.writeInt(entry.getValue());
        }
    }

    /**
     * Reads storage node group metadata via the {@link DataInput} provided.
     * @param in The interface that reads bytes from a binary stream and converts it
     *        to the data of required type.
     * @return Returns the storage node's {@code GroupDescriptor}.
     * @throws IOException thrown if the read fails.
     */
    public static GroupDescriptor readFrom(DataInput in) throws IOException {
        byte version = in.readByte();

        if (version != VERSION) {
            throw new IOException("unsupported version");
        }

        int size = in.readInt();
        Map<String, Integer> groups = new HashMap<>();
        for (int i = 0; i < size; i++) {
            String connectString = in.readUTF();
            Integer groupId = in.readInt();
            groups.put(connectString, groupId);
        }

        return new GroupDescriptor(groups, false);
    }

    @Override
    public int hashCode() {
        return groups.hashCode();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o instanceof GroupDescriptor) {
            GroupDescriptor other = (GroupDescriptor) o;

            if (!this.groups.keySet().equals(other.groups.keySet())) {
                return false;
            }
            for (Map.Entry<String, Integer> entry : this.groups.entrySet()) {
                if (entry.getValue().intValue() != other.groups.get(entry.getKey()).intValue()) {
                    return false;
                }
            }
            return true;
        } else {
            return false;
        }
    }

}
