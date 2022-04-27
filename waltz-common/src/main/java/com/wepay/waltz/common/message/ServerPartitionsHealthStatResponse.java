package com.wepay.waltz.common.message;

import java.util.Map;

public class ServerPartitionsHealthStatResponse extends AbstractMessage {

    public final Map<Integer, Boolean> serverPartitionHealthStats;

    public ServerPartitionsHealthStatResponse(ReqId reqId, Map<Integer, Boolean> partitionHealthStats) {
        super(reqId);
        this.serverPartitionHealthStats = partitionHealthStats;
    }

    @Override
    public byte type() {
        return MessageType.SERVER_PARTITIONS_HEALTH_STAT_RESPONSE;
    }
}
