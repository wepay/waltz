package com.wepay.waltz.server.internal;

import org.junit.Test;

import java.util.Random;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class LocksTest {

    private int[] noLocks = new int[0];
    private Random rand = new Random();

    @Test
    public void testSingleLock() {
        Locks locks = new Locks(100, 3, -1L);
        long transactionId = rand.nextInt(Integer.MAX_VALUE);
        Locks.LockRequest lockRequest;

        // A write lock
        for (int i = 0; i < 10; i++) {
            lockRequest = Locks.createRequest(array(rand.nextInt(Integer.MAX_VALUE)), noLocks, noLocks);

            assertTrue(locks.begin(lockRequest));
            assertTrue(locks.getLockHighWaterMark(lockRequest) <= transactionId);
            locks.commit(lockRequest, transactionId);
            locks.end(lockRequest);

            assertTrue(locks.begin(lockRequest));
            assertEquals(transactionId, locks.getLockHighWaterMark(lockRequest));
            locks.end(lockRequest);

            assertTrue(locks.begin(lockRequest));
            assertEquals(transactionId, locks.getLockHighWaterMark(lockRequest));
            locks.end(lockRequest);

            transactionId++;
        }

        // A read lock
        for (int i = 0; i < 10; i++) {
            // Get the lock high water mark using a write lock. (Do not commit)
            int lock = rand.nextInt(Integer.MAX_VALUE);
            lockRequest = Locks.createRequest(array(lock), noLocks, noLocks);
            assertTrue(locks.begin(lockRequest));
            long lockHighWaterMark = locks.getLockHighWaterMark(lockRequest);
            locks.end(lockRequest);

            // Get the lock high-water make using a read lock, and compare. Do commit.
            lockRequest = Locks.createRequest(noLocks, array(lock), noLocks);
            assertTrue(locks.begin(lockRequest));
            assertEquals(lockHighWaterMark, locks.getLockHighWaterMark(lockRequest));
            locks.commit(lockRequest, transactionId);
            locks.end(lockRequest);

            // Repeat. Commit should not have change the lock high-water mark.
            lockRequest = Locks.createRequest(noLocks, array(lock), noLocks);
            assertTrue(locks.begin(lockRequest));
            assertEquals(lockHighWaterMark, locks.getLockHighWaterMark(lockRequest));
            locks.commit(lockRequest, transactionId);
            locks.end(lockRequest);
        }

        // An append lock
        for (int i = 0; i < 10; i++) {
            // Get the lock high water mark using a write lock. (Do not commit)
            int lock = rand.nextInt(Integer.MAX_VALUE);
            lockRequest = Locks.createRequest(array(lock), noLocks, noLocks);
            assertTrue(locks.begin(lockRequest));
            long lockHighWaterMark = locks.getLockHighWaterMark(lockRequest);
            locks.end(lockRequest);

            // Get the lock high-water make using an append lock, and compare. Do commit.
            lockRequest = Locks.createRequest(noLocks, noLocks, array(lock));
            assertTrue(locks.begin(lockRequest));
            assertEquals(-1L, locks.getLockHighWaterMark(lockRequest));
            locks.commit(lockRequest, transactionId);
            locks.end(lockRequest);

            // Get the lock high-water make using a read lock, and compare.
            lockRequest = Locks.createRequest(noLocks, array(lock), noLocks);
            assertTrue(locks.begin(lockRequest));
            assertEquals(transactionId, locks.getLockHighWaterMark(lockRequest));
            locks.end(lockRequest);
        }
    }

    @Test
    public void testMultipleWriteLocks() {
        Locks locks = new Locks(100, 3, -1L);
        long transactionId1 = rand.nextInt(Integer.MAX_VALUE);
        long transactionId2 = transactionId1 + 1;
        long transactionId3 = transactionId1 + 2;

        for (int i = 0; i < 10; i++) {
            int accountId = rand.nextInt(Integer.MAX_VALUE);
            int paymentId = rand.nextInt(Integer.MAX_VALUE);
            Locks.LockRequest lockRequest;

            // Commit accountId & paymentId at transactionId1
            lockRequest = Locks.createRequest(array(accountId, paymentId), noLocks, noLocks);
            assertTrue(locks.begin(lockRequest));
            assertTrue(locks.getLockHighWaterMark(lockRequest) <= transactionId1);
            locks.commit(lockRequest, transactionId1);
            locks.end(lockRequest);

            // The lock high-water mark for accountId is at transactionId1
            lockRequest = Locks.createRequest(array(accountId), noLocks, noLocks);
            assertTrue(locks.begin(lockRequest));
            assertEquals(transactionId1, locks.getLockHighWaterMark(lockRequest));
            locks.end(lockRequest);

            // The lock high-water mark for paymentId is at transactionId1
            lockRequest = Locks.createRequest(array(paymentId), noLocks, noLocks);
            assertTrue(locks.begin(lockRequest));
            assertEquals(transactionId1, locks.getLockHighWaterMark(lockRequest));
            locks.end(lockRequest);

            // The lock high-water mark for accountId & paymentId is at transactionId1
            lockRequest = Locks.createRequest(array(accountId, paymentId), noLocks, noLocks);
            assertTrue(locks.begin(lockRequest));
            assertEquals(transactionId1, locks.getLockHighWaterMark(lockRequest));
            locks.end(lockRequest);

            // Commit just accountId at transactionId2
            lockRequest = Locks.createRequest(array(accountId), noLocks, noLocks);
            assertTrue(locks.begin(lockRequest));
            assertTrue(locks.getLockHighWaterMark(lockRequest) < transactionId2);
            locks.commit(lockRequest, transactionId2);
            locks.end(lockRequest);

            // The lock high-water mark for accountId is advanced to transactionId2
            lockRequest = Locks.createRequest(array(accountId), noLocks, noLocks);
            assertTrue(locks.begin(lockRequest));
            assertEquals(transactionId2, locks.getLockHighWaterMark(lockRequest));
            locks.end(lockRequest);

            // The lock high-water mark for paymentId is still at transactionId1
            lockRequest = Locks.createRequest(array(paymentId), noLocks, noLocks);
            assertTrue(locks.begin(lockRequest));
            assertEquals(transactionId1, locks.getLockHighWaterMark(lockRequest));
            locks.end(lockRequest);

            // The lock high-water mark for accountId & paymentId is advanced to transactionId2
            lockRequest = Locks.createRequest(array(accountId, paymentId), noLocks, noLocks);
            assertTrue(locks.begin(lockRequest));
            assertEquals(transactionId2, locks.getLockHighWaterMark(lockRequest));
            locks.end(lockRequest);

            // Commit just paymentId at transactionId3
            lockRequest = Locks.createRequest(array(paymentId), noLocks, noLocks);
            assertTrue(locks.begin(lockRequest));
            assertTrue(locks.getLockHighWaterMark(lockRequest) < transactionId3);
            locks.commit(lockRequest, transactionId3);
            locks.end(lockRequest);

            // The lock high-water mark for accountId is still at transactionId2
            lockRequest = Locks.createRequest(array(accountId), noLocks, noLocks);
            assertTrue(locks.begin(lockRequest));
            assertEquals(transactionId2, locks.getLockHighWaterMark(lockRequest));
            locks.end(lockRequest);

            // The lock high-water mark for paymentId is is advanced to transactionId3
            lockRequest = Locks.createRequest(array(paymentId), noLocks, noLocks);
            assertTrue(locks.begin(lockRequest));
            assertEquals(transactionId3, locks.getLockHighWaterMark(lockRequest));
            locks.end(lockRequest);

            // The lock high-water mark for accountId & paymentId is advanced to transactionId3
            lockRequest = Locks.createRequest(array(accountId, paymentId), noLocks, noLocks);
            assertTrue(locks.begin(lockRequest));
            assertEquals(transactionId3, locks.getLockHighWaterMark(lockRequest));
            locks.end(lockRequest);

            transactionId1 += 3;
            transactionId2 += 3;
            transactionId3 += 3;
        }
    }

    @Test
    public void testMultipleReadLocks() {
        Locks locks = new Locks(100, 3, -1L);
        long transactionId1 = rand.nextInt(Integer.MAX_VALUE);
        long transactionId2 = transactionId1 + 1;
        long transactionId3 = transactionId1 + 2;

        for (int i = 0; i < 10; i++) {
            int accountId = rand.nextInt(Integer.MAX_VALUE);
            int paymentId = rand.nextInt(Integer.MAX_VALUE);
            Locks.LockRequest lockRequest;

            // Commit accountId & paymentId at transactionId1
            lockRequest = Locks.createRequest(array(accountId, paymentId), noLocks, noLocks);
            assertTrue(locks.begin(lockRequest));
            assertTrue(locks.getLockHighWaterMark(lockRequest) <= transactionId1);
            locks.commit(lockRequest, transactionId1);
            locks.end(lockRequest);

            // The lock high-water mark for accountId is at transactionId1
            lockRequest = Locks.createRequest(noLocks, array(accountId), noLocks);
            assertTrue(locks.begin(lockRequest));
            assertEquals(transactionId1, locks.getLockHighWaterMark(lockRequest));
            locks.end(lockRequest);

            // The lock high-water mark for paymentId is at transactionId1
            lockRequest = Locks.createRequest(noLocks, array(paymentId), noLocks);
            assertTrue(locks.begin(lockRequest));
            assertEquals(transactionId1, locks.getLockHighWaterMark(lockRequest));
            locks.end(lockRequest);

            // The lock high-water mark for accountId & paymentId is at transactionId1
            lockRequest = Locks.createRequest(noLocks, array(accountId, paymentId), noLocks);
            assertTrue(locks.begin(lockRequest));
            assertEquals(transactionId1, locks.getLockHighWaterMark(lockRequest));
            locks.end(lockRequest);

            lockRequest = Locks.createRequest(array(accountId), array(paymentId), noLocks);
            assertEquals(transactionId1, locks.getLockHighWaterMark(lockRequest));
            locks.end(lockRequest);

            // Commit just accountId at transactionId2
            lockRequest = Locks.createRequest(array(accountId), noLocks, noLocks);
            assertTrue(locks.begin(lockRequest));
            assertTrue(locks.getLockHighWaterMark(lockRequest) < transactionId2);
            locks.commit(lockRequest, transactionId2);
            locks.end(lockRequest);

            // The lock high-water mark for accountId is advanced to transactionId2
            lockRequest = Locks.createRequest(noLocks, array(accountId), noLocks);
            assertTrue(locks.begin(lockRequest));
            assertEquals(transactionId2, locks.getLockHighWaterMark(lockRequest));
            locks.end(lockRequest);

            // The lock high-water mark for paymentId is still at transactionId1
            lockRequest = Locks.createRequest(noLocks, array(paymentId), noLocks);
            assertTrue(locks.begin(lockRequest));
            assertEquals(transactionId1, locks.getLockHighWaterMark(lockRequest));
            locks.end(lockRequest);

            // The lock high-water mark for accountId & paymentId is advanced to transactionId2
            lockRequest = Locks.createRequest(noLocks, array(accountId, paymentId), noLocks);
            assertTrue(locks.begin(lockRequest));
            assertEquals(transactionId2, locks.getLockHighWaterMark(lockRequest));
            locks.end(lockRequest);

            // Commit just paymentId at transactionId3
            lockRequest = Locks.createRequest(array(paymentId), noLocks, noLocks);
            assertTrue(locks.begin(lockRequest));
            assertTrue(locks.getLockHighWaterMark(lockRequest) < transactionId3);
            locks.commit(lockRequest, transactionId3);
            locks.end(lockRequest);

            // The lock high-water mark for accountId is still at transactionId2
            lockRequest = Locks.createRequest(noLocks, array(accountId), noLocks);
            assertTrue(locks.begin(lockRequest));
            assertEquals(transactionId2, locks.getLockHighWaterMark(lockRequest));
            locks.end(lockRequest);

            // The lock high-water mark for paymentId is is advanced to transactionId3
            lockRequest = Locks.createRequest(noLocks, array(paymentId), noLocks);
            assertTrue(locks.begin(lockRequest));
            assertEquals(transactionId3, locks.getLockHighWaterMark(lockRequest));
            locks.end(lockRequest);

            // The lock high-water mark for accountId & paymentId is advanced to transactionId3
            lockRequest = Locks.createRequest(noLocks, array(accountId, paymentId), noLocks);
            assertTrue(locks.begin(lockRequest));
            assertEquals(transactionId3, locks.getLockHighWaterMark(lockRequest));
            locks.end(lockRequest);

            lockRequest = Locks.createRequest(array(accountId), array(paymentId), noLocks);
            assertTrue(locks.begin(lockRequest));
            assertEquals(transactionId3, locks.getLockHighWaterMark(lockRequest));
            locks.end(lockRequest);

            transactionId1 += 3;
            transactionId2 += 3;
            transactionId3 += 3;
        }
    }

    @Test
    public void testHierarchicalResourceScenario() {
        Locks locks = new Locks(100, 3, -1L);
        long getResourceFirstTime = rand.nextInt(Integer.MAX_VALUE);
        long modifyChildResourceFirstTime = getResourceFirstTime + 1;
        long modifyChildResourceSecondTime = getResourceFirstTime + 2;
        long getParentResourceSecondTime = getResourceFirstTime + 3;
        Locks.LockRequest lockRequest;

        final int parent = 12345; // a parent resource
        final int child = 6789;   // a child resource

        // get the resource
        lockRequest = Locks.createRequest(array(parent, child), noLocks, noLocks);
        assertTrue(locks.begin(lockRequest));
        assertEquals(-1L, locks.getLockHighWaterMark(lockRequest));
        locks.commit(lockRequest, getResourceFirstTime);
        locks.end(lockRequest);

        // first modification of the child resource
        lockRequest = Locks.createRequest(noLocks, array(parent), array(child));
        assertTrue(locks.begin(lockRequest));
        // Lock high water-mark be getResourceFirstTime
        assertEquals(getResourceFirstTime, locks.getLockHighWaterMark(lockRequest));
        locks.commit(lockRequest, modifyChildResourceFirstTime);
        locks.end(lockRequest);

        // second modification of the child resource
        lockRequest = Locks.createRequest(noLocks, array(parent), array(child));
        assertTrue(locks.begin(lockRequest));
        // Lock high water-mark should be getResourceFirstTime
        assertEquals(getResourceFirstTime, locks.getLockHighWaterMark(lockRequest));
        locks.commit(lockRequest, modifyChildResourceSecondTime);
        locks.end(lockRequest);

        // get the resource
        lockRequest = Locks.createRequest(array(parent, child), noLocks, noLocks);
        assertTrue(locks.begin(lockRequest));
        // Lock high water-mark should be modifyChildResourceSecondTime.
        assertEquals(modifyChildResourceSecondTime, locks.getLockHighWaterMark(lockRequest));
        locks.commit(lockRequest, getParentResourceSecondTime);
        locks.end(lockRequest);
    }

    @Test
    public void testCollisions() {
        int size = 300;
        int numHashFuncs = 3;
        int numAccounts = 101;

        Locks locks = new Locks(size, numHashFuncs, -1L);
        long clientHighWaterMark = 123;
        long transactionId = clientHighWaterMark + 1;

        int numLockFailures = 0;
        while (numLockFailures == 0) {
            int accountId = rand.nextInt(Integer.MAX_VALUE);
            Locks.LockRequest lockRequest = Locks.createRequest(array(accountId), noLocks, noLocks);

            assertTrue(locks.begin(lockRequest));
            if (locks.getLockHighWaterMark(lockRequest) <= clientHighWaterMark) {
                locks.commit(lockRequest, transactionId++);
            } else {
                numLockFailures++;
            }
            locks.end(lockRequest);
        }

        assertTrue("numLockFailures=" + numLockFailures, 0 < numLockFailures && numLockFailures < numAccounts);
    }

    private int[] array(int... arr) {
        return arr;
    }

}
