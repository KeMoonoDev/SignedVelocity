package io.github._4drian3d.signedvelocity.common.queue;

import io.github._4drian3d.signedvelocity.shared.PropertyHolder;
import org.jetbrains.annotations.Nullable;

import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;

public final class QueuedData {
    private static final int timeout = PropertyHolder.readInt("io.github._4drian3d.signedvelocity.timeout", 20);
    private final Queue<SignedResult> results = new ConcurrentLinkedQueue<>();
    private final Queue<CompletableFuture<SignedResult>> unSyncronizedQueue = new ConcurrentLinkedQueue<>();

    public void complete(final SignedResult result) {
        final var unSynchronized = unSyncronizedQueue.poll();
        if (unSynchronized != null) {
            // If timeout already completed the future, discard the late result
            unSynchronized.complete(result);
            return;
        }
        this.results.add(result);
    }

    public CompletableFuture<SignedResult> nextResult(boolean isCommand) {
        final SignedResult result = results.poll();
        return futureFrom(result, isCommand);
    }

    public CompletableFuture<SignedResult> nextResultWithoutAdvance(boolean isCommand) {
        final SignedResult result = results.peek();
        return futureFrom(result, isCommand);
    }

    private CompletableFuture<SignedResult> futureFrom(final @Nullable SignedResult result, boolean isCommand) {
        if (result == null) {
            final CompletableFuture<SignedResult> future = new CompletableFuture<>();
            future.completeOnTimeout(isCommand ? SignedResult.allowed() : SignedResult.cancel(), timeout, TimeUnit.MILLISECONDS);
            unSyncronizedQueue.add(future);
            return future;
        } else {
            return CompletableFuture.completedFuture(result);
        }
    }
}
