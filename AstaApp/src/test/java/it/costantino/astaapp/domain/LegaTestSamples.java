package it.costantino.astaapp.domain;

import java.util.Random;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

public class LegaTestSamples {

    private static final Random random = new Random();
    private static final AtomicLong longCount = new AtomicLong(random.nextInt() + (2 * Integer.MAX_VALUE));

    public static Lega getLegaSample1() {
        return new Lega().id(1L).nome("nome1").budget(1L);
    }

    public static Lega getLegaSample2() {
        return new Lega().id(2L).nome("nome2").budget(2L);
    }

    public static Lega getLegaRandomSampleGenerator() {
        return new Lega().id(longCount.incrementAndGet()).nome(UUID.randomUUID().toString()).budget(longCount.incrementAndGet());
    }
}
