package io.debezium.embedded.reactive;

import io.debezium.engine.RecordCommitter;
import io.debezium.engine.reactive.CommittableRecord;
import io.reactivex.rxjava3.annotations.NonNull;
import io.reactivex.rxjava3.core.FlowableEmitter;
import io.reactivex.rxjava3.core.FlowableOnSubscribe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A Flowable able to produce record elements.
 *
 * It's responsible for polling data in parallel and stopping the stream if an error occurs.
 */
public class DebeziumFlowableOnSubscribe<R> implements FlowableOnSubscribe<CommittableRecord<R>> {
    private static final Logger LOGGER = LoggerFactory.getLogger(DebeziumFlowableOnSubscribe.class);

    private final RecordSupplier<R> pollF;
    private final RecordCommitter<R> committer;

    private final AtomicBoolean shouldSpawn;
    private final AtomicInteger parallelRequests;
    private final int maxConcurrency;

    private final ExecutorService schedulingExecutor = Executors.newSingleThreadExecutor();
    private final ExecutorService executorService;

    private final AtomicBoolean stopped;

    public DebeziumFlowableOnSubscribe(int maxConcurrency, RecordSupplier<R> pollF, RecordCommitter<R> committer) {
        if (maxConcurrency < 1) {
            throw new IllegalArgumentException("maxConcurrency must be higher than 0");
        }

        this.maxConcurrency   = maxConcurrency;
        this.pollF            = pollF;
        this.committer        = committer;

        this.parallelRequests = new AtomicInteger(0);
        this.shouldSpawn      = new AtomicBoolean(false);
        this.stopped          = new AtomicBoolean(false);

        this.executorService  = Executors.newFixedThreadPool(maxConcurrency);
    }

    @Override
    public void subscribe(@NonNull FlowableEmitter<CommittableRecord<R>> emitter) {
        schedulingExecutor.execute(() -> {
            while (true) {
                if (emitter.isCancelled()) {
                    schedulingExecutor.shutdown();
                    executorService.shutdown();
                    break;
                }

                if (stopped.get()) {
                    schedulingExecutor.shutdown();
                    executorService.shutdown();
                    emitter.onComplete();
                    break;
                }

                if (parallelRequests.get() == maxConcurrency) {
                    continue;
                }

                if (emitter.requested() == 0) {
                    continue;
                }

                final boolean shouldRun = shouldSpawn.get() || parallelRequests.incrementAndGet() == 1;

                if (shouldRun) {
                    executorService.execute(() -> {
                        try {
                            LOGGER.debug("Polling task for records on thread {}", Thread.currentThread());
                            final List<R> offset = pollF.poll();
                            LOGGER.debug("{} records were found while polling", offset.size());

                            shouldSpawn.set(!offset.isEmpty());

                            final DefaultOffsetCommitter<R> offsetCommitter = new DefaultOffsetCommitter<>(offset, committer);

                            offset.stream()
                                    .map(r -> new DefaultCommittableRecord<>(offsetCommitter, r))
                                    .forEach(emitter::onNext);

                            parallelRequests.decrementAndGet();
                        } catch (InterruptedException e) {
                            stopped.set(true);
                            LOGGER.error("An error occurred while polling for new records: {}", e.getMessage(), e);
                            LOGGER.error("Producer will now stop");
                            emitter.onError(e);
                        }
                    });
                }
            }
        });
    }
}
