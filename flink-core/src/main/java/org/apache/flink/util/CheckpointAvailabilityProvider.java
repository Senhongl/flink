/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.util;

/** The interface for an operator to check its availability of taking snapshots. */
public interface CheckpointAvailabilityProvider {
    /**
     * This method is called to check the state to indicate that if it is able to take a snapshot
     * and what kind of failure should be reported.
     *
     * <p>When the state is not allowed to take a checkpoint or the checkpoint could be skipped. For
     * example,
     *
     * <ul>
     *   <li>the current data might be changed after recovery from the current checkpoint.
     *   <li>the current operator is under high backpressure and taking a snapshot could take a long
     *       time.
     * </ul>
     *
     * <p>Then the operator could simply reject the current checkpoint and wait for the next
     * checkpoint triggered.
     *
     * <p>Two kinds of reject reason could be returned:
     *
     * <ul>
     *   <li>Soft failure, which means that it is acceptable/predictable by the users/developers to
     *       reject this checkpoint.
     *   <li>Hard failure, which means that the checkpoint failure, either checkpoint rejection or
     *       other failure reasons, is unacceptable/unpredictable by the users/developers. For
     *       example, an operator is allowed to reject a checkpoint for a period of time, but every
     *       rejection outside that time range will be treated as a hard failure.
     * </ul>
     *
     * <p>If a soft failure is returned, the coordinator would ignore it. If a hard failure is
     * returned, the coordinator would keep tracking it.
     *
     * @param checkpointID The ID of the current checkpoint.
     * @return indicating that if the Operator is able to take a snapshot and if not, what kind of
     *     failure should be reported. If SNAPSHOT_AVAILABLE is returned, then the subtask would
     *     continue doing the checkpoint. Otherwise, this checkpoint would be rejected and report
     *     the soft/hard failure to the coordinator.
     */
    SnapshotAvailability isSnapshotAvailable(long checkpointID);
}
