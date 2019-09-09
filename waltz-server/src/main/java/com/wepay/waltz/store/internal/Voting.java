package com.wepay.waltz.store.internal;

/**
 * This class handles voting to maintain quorum during
 * replication.
 */
public class Voting {

    private final int quorum;
    private final int maxAbstentions;

    private int numVotes = 0;
    private int numAbstentions = 0;

    /**
     * Class constructor.
     * @param quorum The quorum to maintain.
     * @param numVoters The number of voters.
     */
    public Voting(int quorum, int numVoters) {
        this.quorum = quorum;
        this.maxAbstentions = numVoters - quorum;
    }

    /**
     * Perform voting.
     */
    public void vote() {
        synchronized (this) {
            if (++numVotes >= quorum) {
                notifyAll();
            }
        }
    }

    /**
     * Removes voter from voting.
     */
    public void abstain() {
        synchronized (this) {
            if (++numAbstentions > maxAbstentions) {
                notifyAll();
            }
        }
    }

    /**
     * Checks if the quorum has been maintained.
     * @return True if quorum is achieved, otherwise returns False.
     */
    public boolean hasQuorum() {
        synchronized (this) {
            return numVotes >= quorum;
        }
    }

    /**
     * Checks if there are any voters that didn't vote.
     * @return True if any voters didn't vote.
     */
    public boolean hasAbstention() {
        synchronized (this) {
            return numAbstentions > 0;
        }
    }

    /**
     * Waits for sometime to see if the quorum has been achieved.
     * @return True if quorum is achieved, otherwise return False.
     */
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
