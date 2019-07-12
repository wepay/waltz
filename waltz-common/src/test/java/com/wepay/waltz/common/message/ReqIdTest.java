package com.wepay.waltz.common.message;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class ReqIdTest {

    private int clientId = 0x11111111;
    private int generation = 0x22222222;
    private int partitionId = 0x33333333;
    private int seqNum = 0x44444444;

    @Test
    public void testFields() {
        ReqId reqId = new ReqId(clientId, generation, partitionId, seqNum);
        assertEquals(clientId, reqId.clientId());
        assertEquals(generation, reqId.generation());
        assertEquals(partitionId, reqId.partitionId());
        assertEquals(seqNum, reqId.seqNum());
    }

    @Test
    public void testEquals() {
        assertTrue(new ReqId(clientId, generation, partitionId, seqNum).equals(new ReqId(clientId, generation, partitionId, seqNum)));
        assertFalse(new ReqId(clientId, generation, partitionId, seqNum).equals(null));
        assertFalse(new ReqId(clientId, generation, partitionId, seqNum).equals(new Object()));
    }

}
