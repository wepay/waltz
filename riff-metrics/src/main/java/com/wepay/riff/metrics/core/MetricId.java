package com.wepay.riff.metrics.core;

/**
 * An unique identification of {@link Metric}.
 */
public final class MetricId implements Comparable<MetricId> {

    private final String group;
    private final String name;

    public MetricId(String group, String name) {
        this.group = group;
        this.name = name;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o instanceof MetricId) {
            MetricId other = (MetricId) o;
            if (!this.group.equals(other.group)) {
                return false;
            }
            if (!this.name.equals(other.name)) {
                return false;
            }
            return true;
        } else {
            return false;
        }
    }

    @Override
    public int hashCode() {
        return group.hashCode() + name.hashCode();
    }

    @Override
    public int compareTo(MetricId other) {
        return this.getFullName().compareTo(other.getFullName());
    }

    @Override
    public String toString() {
        return this.getFullName();
    }

    public String getGroup() {
        return group;
    }

    public String getName() {
        return name;
    }

    public String getFullName() {
        return !group.isEmpty() ? group + "." + name : name;
    }

}
