package it.costantino.astaapp.domain;

import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;

public class RosterTestSamples {

    private static final Random random = new Random();
    private static final AtomicLong longCount = new AtomicLong(random.nextInt() + (2 * Integer.MAX_VALUE));

    public static Roster getRosterSample1() {
        return new Roster().id(1L).port(1L).dif(1L).cc(1L).att(1L);
    }

    public static Roster getRosterSample2() {
        return new Roster().id(2L).port(2L).dif(2L).cc(2L).att(2L);
    }

    public static Roster getRosterRandomSampleGenerator() {
        return new Roster()
            .id(longCount.incrementAndGet())
            .port(longCount.incrementAndGet())
            .dif(longCount.incrementAndGet())
            .cc(longCount.incrementAndGet())
            .att(longCount.incrementAndGet());
    }
}
