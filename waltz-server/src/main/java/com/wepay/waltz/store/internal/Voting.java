package com.wepay.waltz.store.internal;

public class Voting {

    private final int quorum;
    private final int maxAbstentions;

    private int numVotes = 0;
    private int numAbstentions = 0;

    public Voting(int quorum, int numVoters) {
        this.quorum = quorum;
        this.maxAbstentions = numVoters - quorum;
    }

    public void vote() {
        synchronized (this) {
            if (++numVotes >= quorum) {
                notifyAll();
            }
        }
    }

    public void abstain() {
        synchronized (this) {
            if (++numAbstentions > maxAbstentions) {
                notifyAll();
            }
        }
    }

    public boolean hasQuorum() {
        synchronized (this) {
            return numVotes >= quorum;
        }
    }

    public boolean hasAbstention() {
        synchronized (this) {
            return numAbstentions > 0;
        }
    }

    public boolean await() {
        synchronized (this) {
            while ((numVotes < quorum && numAbstentions <= maxAbstentions)) {
                try {
                    wait();
                } catch (InterruptedException ex) {
                    Thread.interrupted();
                }
            }
            return numVotes >= quorum;
        }
    }

    @Override
    public String toString() {
        synchronized (this) {
            return this.getClass().getSimpleName()
                + " [ quorum=" + quorum + " numVotes=" + numVotes + " numAbstention=" + numAbstentions + " ]";
        }
    }

}
