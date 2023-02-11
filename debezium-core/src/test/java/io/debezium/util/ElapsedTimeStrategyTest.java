/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.util;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.Before;
import org.junit.Test;

/**
 * @author Randall Hauch
 *
 */
public class ElapsedTimeStrategyTest {

    private ElapsedTimeStrategy delay;
    private MockClock clock;

    @Before
    public void beforeEach() {
        clock = new MockClock();
    }

    @Test
    public void testConstantDelay() {
        clock.advanceTo(100);
        delay = ElapsedTimeStrategy.constant(clock, 10);
        // Initial call should always be true ...
        assertElapsed();
        // next stop at 100+10=110

        assertNotElapsed();
        clock.advanceTo(109);
        assertNotElapsed();
        clock.advanceTo(110);
        assertElapsed();
        // next stop at 110+10=120

        clock.advanceTo(119);
        assertNotElapsed();
        clock.advanceTo(128);
        assertElapsed();
        // next stop at 120+10=130

        clock.advanceTo(130);
        assertElapsed();
        // next stop at 130+10=140

        clock.advanceTo(139);
        assertNotElapsed();
        clock.advanceTo(140);
        assertElapsed();

        // Advance many multiples should be instantaneous...
        clock.advanceTo(100000000000000L);
        assertElapsed();
    }

    @Test
    public void testLinearDelay() {
        clock.advanceTo(100);
        delay = ElapsedTimeStrategy.linear(clock, Duration.ofMillis(100));
        // Initial call should always be true ...
        assertElapsed();
        // next stop at 100+(100*1)=200
        assertNotElapsed();
        clock.advanceTo(199);
        assertNotElapsed();
        clock.advanceTo(200);
        assertElapsed();
        // next stop at 200+(100*2)=400

        clock.advanceTo(201);
        assertNotElapsed();
        clock.advanceTo(301);
        assertNotElapsed();
        clock.advanceTo(400);
        assertElapsed();
        // next stop at 400+(100*3)=700

        clock.advanceTo(401);
        assertNotElapsed();
        clock.advanceTo(699);
        assertNotElapsed();
        clock.advanceTo(701);
        assertElapsed();
        // next stop at 700+(100*4)=1100

        clock.advanceTo(1099);
        assertNotElapsed();
        clock.advanceTo(1101);
        assertElapsed();

        // Advance many multiples should be instantaneous...
        clock.advanceTo(100000000000000L);
        assertElapsed();
    }

    @Test
    public void testStepDelayStartingBeforeStep() {
        clock.advanceTo(100);
        // start out before the step ...
        AtomicBoolean step = new AtomicBoolean(false);
        delay = ElapsedTimeStrategy.step(clock, Duration.ofMillis(10), step::get, Duration.ofMillis(100));

        // Initial call should always be true ...
        assertElapsed();
        // next stop at 100+(10)=110
        assertNotElapsed();
        clock.advanceTo(109);
        assertNotElapsed();
        clock.advanceTo(110);
        assertElapsed();
        // next stop at 110+(10)=120

        clock.advanceTo(119);
        assertNotElapsed();
        clock.advanceTo(120);
        assertElapsed();
        // next stop at 120+(10)=130

        clock.advanceTo(129);
        assertNotElapsed();

        // trigger the step ...
        step.set(true);
        assertNotElapsed();

        clock.advanceTo(130);
        assertElapsed();
        // next stop at 130+(100)=230

        clock.advanceTo(229);
        assertNotElapsed();
        clock.advanceTo(230);
        assertElapsed();
        // next stop at 230+(100)=330

        clock.advanceTo(329);
        assertNotElapsed();
        // un-trigger the step, but the strategy shouldn't care about this at all
        step.set(false);

        clock.advanceTo(330);
        assertElapsed();
        // next stop at 330+(100)=430

        clock.advanceTo(331);
        assertNotElapsed();
        clock.advanceTo(341);
        assertNotElapsed();
        clock.advanceTo(429);
        assertNotElapsed();
        clock.advanceTo(430);
        assertElapsed();

        // Advance many multiples should be instantaneous...
        clock.advanceTo(100000000000000L);
        assertElapsed();
    }

    @Test
    public void testStepDelayStartingAfterStep() {
        clock.advanceTo(100);
        // start out before the step ...
        AtomicBoolean step = new AtomicBoolean(true);
        delay = ElapsedTimeStrategy.step(clock, Duration.ofMillis(10), step::get, Duration.ofMillis(100));

        // Initial call should always be true ...
        assertElapsed();
        // next stop at 100+(100)=200
        assertNotElapsed();
        clock.advanceTo(109);
        assertNotElapsed();
        clock.advanceTo(110);
        assertNotElapsed();
        clock.advanceTo(199);
        assertNotElapsed();
        clock.advanceTo(200);
        assertElapsed();
        // next stop at 200+(100)=300

        clock.advanceTo(209);
        assertNotElapsed();
        clock.advanceTo(300);
        assertElapsed();
        // next stop at 300+(100)=400

        // un-trigger the step, but the strategy shouldn't care about this at all
        step.set(false);
        assertNotElapsed();

        clock.advanceTo(399);
        assertNotElapsed();
        clock.advanceTo(400);
        assertElapsed();
        // next stop at 400+(100)=500

        clock.advanceTo(409);
        assertNotElapsed();
        clock.advanceTo(410);
        assertNotElapsed();
        clock.advanceTo(499);
        assertNotElapsed();
        clock.advanceTo(500);
        assertElapsed();

        // trigger the step, but the strategy shouldn't care about this at all
        step.set(true);
        clock.advanceTo(501);
        assertNotElapsed();
        clock.advanceTo(510);
        assertNotElapsed();
        clock.advanceTo(599);
        assertNotElapsed();
        clock.advanceTo(600);
        assertElapsed();

        // Advance many multiples should be instantaneous...
        clock.advanceTo(100000000000000L);
        assertElapsed();
    }

    @Test
    public void testExponentialDelay() {
        clock.advanceTo(100);
        delay = ElapsedTimeStrategy.exponential(clock, Duration.ofMillis(100), Duration.ofMillis(4000));
        // Initial call should always be true ...
        assertElapsed();
        // next stop at 100+(100)=200

        assertNotElapsed();
        clock.advanceTo(199);
        assertNotElapsed();
        clock.advanceTo(200);
        assertElapsed();
        // next stop at 200+(100*2)=400

        clock.advanceTo(201);
        assertNotElapsed();
        clock.advanceTo(301);
        assertNotElapsed();
        clock.advanceTo(400);
        assertElapsed();
        // next stop at 400+(200*2)=800

        clock.advanceTo(401);
        assertNotElapsed();
        clock.advanceTo(799);
        assertNotElapsed();
        clock.advanceTo(800);
        assertElapsed();
        // next stop at 800+(400*2)=1600

        clock.advanceTo(801);
        assertNotElapsed();
        clock.advanceTo(1599);
        assertNotElapsed();
        clock.advanceTo(1600);
        assertElapsed();
        // next stop at 1600+(800*2)=3200

        clock.advanceTo(1601);
        assertNotElapsed();
        clock.advanceTo(3199);
        assertNotElapsed();
        clock.advanceTo(3200);
        assertElapsed();
        // next stop at 3200+(1600*2)=6400

        clock.advanceTo(3201);
        assertNotElapsed();
        clock.advanceTo(6399);
        assertNotElapsed();
        clock.advanceTo(6400);
        assertElapsed();
        // next stop at 6400+(3200*2)=12800, but max delta is 4000, so
        // next stop at 6400+(4000)=10400

        clock.advanceTo(6401);
        assertNotElapsed();
        clock.advanceTo(10399);
        assertNotElapsed();
        clock.advanceTo(10400);
        assertElapsed();

        // Advance many multiples should be instantaneous...
        clock.advanceTo(100000000000000L);
        assertElapsed();
        clock.advanceTo(100000000000001L);
        assertNotElapsed();
        clock.advanceTo(100000000006400L);
        assertElapsed();
    }

    protected void assertElapsed() {
        assertThat(delay.hasElapsed()).isTrue();
        assertNotElapsed();
    }

    protected void assertNotElapsed() {
        for (int i = 0; i != 5; ++i) {
            assertThat(delay.hasElapsed()).isFalse();
        }
    }

}
