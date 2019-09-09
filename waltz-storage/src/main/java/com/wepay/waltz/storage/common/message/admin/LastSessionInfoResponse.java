package com.wepay.waltz.storage.common.message.admin;

import com.wepay.waltz.storage.common.SessionInfo;

public class LastSessionInfoResponse extends AdminMessage {

    public final int partitionId;
    public final SessionInfo lastSessionInfo;

    public LastSessionInfoResponse(long seqNum, int partitionId, SessionInfo lastSessionInfo) {
        super(seqNum);

        this.partitionId = partitionId;
        this.lastSessionInfo = lastSessionInfo;
    }

    @Override
    public byte type() {
        return AdminMessageType.LAST_SESSION_INFO_RESPONSE;
    }
}
