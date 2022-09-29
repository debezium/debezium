package io.debezium.connector.mysql;

import java.util.Timer;
import java.util.TimerTask;

/**
 * Class to schedule the log.
 * TODO: Change to Exectors
 */
public class Scheduler {
    private final Timer t = new Timer();

    public TimerTask schedule(final Runnable r, long delay, long period) {
        final TimerTask task = new TimerTask() {
            public void run() {
                r.run();
            }
        };
        t.scheduleAtFixedRate(task, delay,period);
        return task;
    }
}