package org.apache.flink.util;

/**
 * The interface for an operator to check its availability of taking snapshots.
 */
public interface CheckpointAvailabilityProvider {
    /**
     * This method is called to check the state to indicate that if it is able to take a snapshot and what
     * kind of failure should be reported.
     *
     * <p> When the state is not allowed to take a checkpoint or the checkpoint could be skipped.
     * For example,
     * <ul>
     *     <li> the current data might be changed after recovery from the current checkpoint.
     *     <li> the current operator is under high backpressure and taking a snapshot could take a
     *          long time.
     * </ul>
     *
     * <p> Then the operator could simply reject the current checkpoint and wait for the next checkpoint
     * triggered.
     *
     * <p> Two kinds of reject reason could be returned:
     * <ul>
     *     <li> Soft failure, which means that it is acceptable/predictable by the users/developers to
     *          reject this checkpoint.
     *     <li> Hard failure, which means that the current checkpoint should succeed and should not
     *          be rejected. For example, an 1 operator is allowed to reject a checkpoint for a period of
     *          time, but every rejection outside that time range will be treated as a hard failure.
     * </ul>
     *
     * <p> If a soft failure is returned, the coordinator would treat it differently from the normal failure,
     * which is hard failure. But the number of tolerable continuous soft failures or the timeout of
     * tolerable continuous soft failures have to be configured. If a hard failure is returned, the current
     * handling mechanism is sophisticated enough.
     *
     * @param checkpointID The ID of the current checkpoint.
     *
     * @return indicating that if the Operator is able to take a snapshot and if not, what kind of failure
     * should be reported. If SNAPSHOT_AVAILABLE is returned, then the subtask would continue doing the checkpoint.
     * Otherwise, this checkpoint would be rejected and report the soft/hard failure to the coordinator.
     */
    UnavailableOption isSnapshotAvailable(long checkpointID);
}
