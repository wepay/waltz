package com.wepay.waltz.store.internal;

import com.wepay.zktools.util.Uninterruptibly;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class VotingTest {

    @Test
    public void testVotes() throws Exception {
        for (int numVoters = 1; numVoters < 10; numVoters++) {
            int quorum = (numVoters + 2) / 2;
            int maxAbstentions = numVoters - quorum;

            Voting voting = new Voting(quorum, numVoters);
            assertFalse(voting.hasQuorum());
            assertFalse(voting.hasAbstention());

            CountDownLatch latch = new CountDownLatch(1);
            Thread thread = new Thread(() -> {
                Uninterruptibly.run(voting::await);
                latch.countDown();
            });

            thread.start();

            for (int i = 0; i < quorum - 1; i++) {
                assertTrue(thread.isAlive());
                voting.vote();
                assertFalse(voting.hasQuorum());
                assertFalse(voting.hasAbstention());
            }

            for (int i = 0; i < maxAbstentions; i++) {
                assertTrue(thread.isAlive());
                voting.abstain();
                assertFalse(voting.hasQuorum());
                assertTrue(voting.hasAbstention());
            }

            assertFalse(latch.await(10, TimeUnit.MILLISECONDS));
            assertTrue(thread.isAlive());
            assertFalse(voting.hasQuorum());

            voting.vote();

            assertTrue(voting.hasQuorum());

            assertTrue(latch.await(10, TimeUnit.SECONDS));
        }
    }

    @Test
    public void testAbstentions() throws Exception {
        for (int numVoters = 3; numVoters < 10; numVoters++) {
            int quorum = (numVoters + 2) / 2;
            int maxAbstentions = numVoters - quorum;

            Voting voting = new Voting(quorum, numVoters);
            assertFalse(voting.hasQuorum());
            assertFalse(voting.hasAbstention());

            CountDownLatch latch = new CountDownLatch(1);
            Thread thread = new Thread(() -> {
                Uninterruptibly.run(voting::await);
                latch.countDown();
            });

            thread.start();

            for (int i = 0; i < maxAbstentions; i++) {
                assertTrue(thread.isAlive());
                voting.abstain();
                assertTrue(voting.hasAbstention());
                assertFalse(voting.hasQuorum());
            }

            for (int i = 0; i < quorum - 1; i++) {
                assertTrue(thread.isAlive());
                voting.vote();
                assertTrue(voting.hasAbstention());
                assertFalse(voting.hasQuorum());
            }

            assertFalse(latch.await(10, TimeUnit.MILLISECONDS));
            assertTrue(thread.isAlive());
            assertFalse(voting.hasQuorum());

            voting.abstain();

            assertFalse(voting.hasQuorum());
            assertTrue(voting.hasAbstention());

            assertTrue(latch.await(10, TimeUnit.SECONDS));
        }
    }

}
