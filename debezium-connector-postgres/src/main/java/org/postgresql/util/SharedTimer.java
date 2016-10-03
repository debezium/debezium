package org.postgresql.util;

import org.postgresql.core.Logger;

import java.util.Timer;
import java.util.concurrent.atomic.AtomicInteger;

public class SharedTimer {
  // Incremented for each Timer created, this allows each to have a unique Timer name
  private static AtomicInteger timerCount = new AtomicInteger(0);

  private Logger log;
  private volatile Timer timer = null;
  private AtomicInteger refCount = new AtomicInteger(0);

  public SharedTimer(Logger log) {
    this.log = log;
  }

  public int getRefCount() {
    return refCount.get();
  }

  public synchronized Timer getTimer() {
    if (timer == null) {
      int index = timerCount.incrementAndGet();
      timer = new Timer("PostgreSQL-JDBC-SharedTimer-" + index, true);
    }
    refCount.incrementAndGet();
    return timer;
  }

  public synchronized void releaseTimer() {
    int count = refCount.decrementAndGet();
    if (count > 0) {
      // There are outstanding references to the timer so do nothing
      log.debug("Outstanding references still exist so not closing shared Timer");
    } else if (count == 0) {
      // This is the last usage of the Timer so cancel it so it's resources can be release.
      log.debug("No outstanding references to shared Timer, will cancel and close it");
      if (timer != null) {
        timer.cancel();
        timer = null;
      }
    } else {
      // Should not get here under normal circumstance, probably a bug in app code.
      log.debug(
          "releaseTimer() called too many times; there is probably a bug in the calling code");
      refCount.set(0);
    }
  }
}
