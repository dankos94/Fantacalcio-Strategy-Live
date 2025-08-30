package it.costantino.astaapp.domain;

import java.util.Random;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

public class StagioneTestSamples {

    private static final Random random = new Random();
    private static final AtomicLong longCount = new AtomicLong(random.nextInt() + (2 * Integer.MAX_VALUE));

    public static Stagione getStagioneSample1() {
        return new Stagione().id(1L).nome("nome1");
    }

    public static Stagione getStagioneSample2() {
        return new Stagione().id(2L).nome("nome2");
    }

    public static Stagione getStagioneRandomSampleGenerator() {
        return new Stagione().id(longCount.incrementAndGet()).nome(UUID.randomUUID().toString());
    }
}
