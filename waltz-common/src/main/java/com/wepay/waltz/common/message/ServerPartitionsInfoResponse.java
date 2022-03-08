package com.wepay.waltz.common.message;

import com.wepay.zktools.clustermgr.PartitionInfo;

import java.util.List;

public class ServerPartitionsInfoResponse extends AbstractMessage {

    public final List<PartitionInfo> serverPartitionInfo;

    public ServerPartitionsInfoResponse(ReqId reqId, List<PartitionInfo> serverPartitionInfo) {
        super(reqId);
        this.serverPartitionInfo = serverPartitionInfo;
    }

    @Override
    public byte type() {
        return MessageType.SERVER_PARTITIONS_INFO_RESPONSE;
    }
}
