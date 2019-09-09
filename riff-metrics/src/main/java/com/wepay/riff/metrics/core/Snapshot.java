package com.wepay.riff.metrics.core;

import java.io.OutputStream;

/**
 * A statistical snapshot of a {@link Snapshot}.
 */
public abstract class Snapshot {
    private static final double MEDIAN = 0.5;
    private static final double SEVENTY_FIFTH_PERCENTILE = 0.75;
    private static final double NIGHTY_FIFTH_PERCENTILE = 0.95;
    private static final double NIGHTY_EIGHTH_PERCENTILE = 0.98;
    private static final double NIGHTY_NINTH_PERCENTILE = 0.99;
    private static final double NINE_NIGHTY_NINTH_PERCENTILE = 0.999;

    /**
     * Returns the value at the given quantile.
     *
     * @param quantile a given quantile, in {@code [0..1]}
     * @return the value in the distribution at {@code quantile}
     */
    public abstract double getValue(double quantile);

    /**
     * Returns the entire set of values in the snapshot.
     *
     * @return the entire set of values
     */
    public abstract long[] getValues();

    /**
     * Returns the number of values in the snapshot.
     *
     * @return the number of values
     */
    public abstract int size();

    /**
     * Returns the median value in the distribution.
     *
     * @return the median value
     */
    public double getMedian() {
        return getValue(MEDIAN);
    }

    /**
     * Returns the value at the 75th percentile in the distribution.
     *
     * @return the value at the 75th percentile
     */
    public double get75thPercentile() {
        return getValue(SEVENTY_FIFTH_PERCENTILE);
    }

    /**
     * Returns the value at the 95th percentile in the distribution.
     *
     * @return the value at the 95th percentile
     */
    public double get95thPercentile() {
        return getValue(NIGHTY_FIFTH_PERCENTILE);
    }

    /**
     * Returns the value at the 98th percentile in the distribution.
     *
     * @return the value at the 98th percentile
     */
    public double get98thPercentile() {
        return getValue(NIGHTY_EIGHTH_PERCENTILE);
    }

    /**
     * Returns the value at the 99th percentile in the distribution.
     *
     * @return the value at the 99th percentile
     */
    public double get99thPercentile() {
        return getValue(NIGHTY_NINTH_PERCENTILE);
    }

    /**
     * Returns the value at the 99.9th percentile in the distribution.
     *
     * @return the value at the 99.9th percentile
     */
    public double get999thPercentile() {
        return getValue(NINE_NIGHTY_NINTH_PERCENTILE);
    }

    /**
     * Returns the highest value in the snapshot.
     *
     * @return the highest value
     */
    public abstract long getMax();

    /**
     * Returns the arithmetic mean of the values in the snapshot.
     *
     * @return the arithmetic mean
     */
    public abstract double getMean();

    /**
     * Returns the lowest value in the snapshot.
     *
     * @return the lowest value
     */
    public abstract long getMin();

    /**
     * Returns the standard deviation of the values in the snapshot.
     *
     * @return the standard value
     */
    public abstract double getStdDev();

    /**
     * Writes the values of the snapshot to the given stream.
     *
     * @param output an output stream
     */
    public abstract void dump(OutputStream output);

}
