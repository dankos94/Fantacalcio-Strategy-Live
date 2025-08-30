package it.costantino.astaapp.domain;

import java.util.Random;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

public class SquadraTestSamples {

    private static final Random random = new Random();
    private static final AtomicLong longCount = new AtomicLong(random.nextInt() + (2 * Integer.MAX_VALUE));

    public static Squadra getSquadraSample1() {
        return new Squadra().id(1L).nome("nome1");
    }

    public static Squadra getSquadraSample2() {
        return new Squadra().id(2L).nome("nome2");
    }

    public static Squadra getSquadraRandomSampleGenerator() {
        return new Squadra().id(longCount.incrementAndGet()).nome(UUID.randomUUID().toString());
    }
}
