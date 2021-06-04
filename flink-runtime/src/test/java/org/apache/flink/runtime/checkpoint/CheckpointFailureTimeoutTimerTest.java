package org.apache.flink.runtime.checkpoint;

import org.apache.flink.runtime.concurrent.ManuallyTriggeredScheduledExecutor;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.executiongraph.ExecutionGraph;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.tasks.CheckpointCoordinatorConfiguration;

import org.junit.Test;

import java.util.concurrent.CompletableFuture;

import static org.junit.Assert.assertEquals;

public class CheckpointFailureTimeoutTimerTest {
    private CheckpointCoordinatorTestingUtils.CheckpointRecorderTaskManagerGateway gateway;
    private CheckpointCoordinator coordinator;
    private TestFailJobCallback callback;
    private ManuallyTriggeredScheduledExecutor manuallyTriggeredScheduledExecutor;

    @Test
    public void timerTest() {

        try {
            initCheckpointCoordinaot(0, 50);

            coordinator.startCheckpointScheduler();

            coordinator.startCheckpointFailuresTimeoutTimer();
            manuallyTriggeredScheduledExecutor.triggerScheduledTasks();
            coordinator.stopCheckpointScheduler();

            assertEquals(1, callback.getFailureTimeoutExceededCounter());
            assertEquals(0, callback.getFailureNumberExceededCounter());
        } catch (Exception e) {

        }
    }



    public void initCheckpointCoordinaot(int tolerableCpFailureNumber,
                                                       long tolerableCpFailureTimeout) throws Exception {
        final JobVertexID jobVertexID1 = new JobVertexID();

        gateway = new CheckpointCoordinatorTestingUtils.CheckpointRecorderTaskManagerGateway();

        final ExecutionGraph graph =
                new CheckpointCoordinatorTestingUtils.CheckpointExecutionGraphBuilder()
                        .setTaskManagerGateway(gateway)
                        .addJobVertex(jobVertexID1)
                        .build();

        manuallyTriggeredScheduledExecutor = new ManuallyTriggeredScheduledExecutor();

        callback = new TestFailJobCallback();
        CheckpointFailureManager failureManager = new CheckpointFailureManager(tolerableCpFailureNumber, callback);

        final CheckpointCoordinatorConfiguration chkConfig =
                new CheckpointCoordinatorConfiguration.CheckpointCoordinatorConfigurationBuilder()
                        .setTolerableCheckpointFailureTimeout(tolerableCpFailureTimeout)
                        .setTolerableCheckpointFailureNumber(tolerableCpFailureNumber)
                        .setCheckpointInterval(10)
                        .setCheckpointTimeout(10)
                        .build();


        coordinator = new CheckpointCoordinatorTestingUtils.CheckpointCoordinatorBuilder()
                        .setExecutionGraph(graph)
                        .setTimer(manuallyTriggeredScheduledExecutor)
                        .setCheckpointCoordinatorConfiguration(chkConfig)
                        .setFailureManager(failureManager)
                        .build();
    }

    private static class TestFailJobCallback implements CheckpointFailureManager.FailJobCallback {

        private int failureTimeoutExceededCounter = 0;
        private int failureNumberExceededCounter = 0;

        @Override
        public void failJob(Throwable cause) {
            if (cause.getMessage().equals(CheckpointFailureManager.TOLERABLE_CHECKPOINT_FAILURES_TIMEOUT_MESSAGE)) {
                failureTimeoutExceededCounter++;
            } else if (cause.getMessage().equals(CheckpointFailureManager.EXCEEDED_CHECKPOINT_TOLERABLE_FAILURE_MESSAGE)) {
                failureNumberExceededCounter++;
            } else {
                // unreachable within this test
            }
        }

        @Override
        public void failJobDueToTaskFailure(
                final Throwable cause, final ExecutionAttemptID executionAttemptID) {
            if (cause.getMessage().equals(CheckpointFailureManager.TOLERABLE_CHECKPOINT_FAILURES_TIMEOUT_MESSAGE)) {
                failureTimeoutExceededCounter++;
            } else if (cause.getMessage().equals(CheckpointFailureManager.EXCEEDED_CHECKPOINT_TOLERABLE_FAILURE_MESSAGE)) {
                failureNumberExceededCounter++;
            } else {
                // unreachable within this test
            }
        }

        public int getFailureNumberExceededCounter() {
            return failureNumberExceededCounter;
        }

        public int getFailureTimeoutExceededCounter() {
            return failureTimeoutExceededCounter;
        }
    }
}
