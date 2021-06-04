package org.apache.flink.util;

/** Various options that can be returned when checking its availability for taking snapshots. */
public enum UnavailableOption {
    SNAPSHOT_AVAILABLE("The current state is available for checkpointing."),

    SNAPSHOT_UNAVAILABLE_SOFT_FAILURE("The current checkpoint is not allowed, but it is acceptable."),

    SNAPSHOT_UNAVAILABLE_HARD_FAILURE("The current checkpoint is not allowed, but it is unacceptable.");

    // ------------------------------------------------------------------------

    private final String message;

    UnavailableOption(String message) {
        this.message = message;
    }
}
