package it.costantino.astaapp.domain;

import java.util.Random;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

public class CoachTestSamples {

    private static final Random random = new Random();
    private static final AtomicLong longCount = new AtomicLong(random.nextInt() + (2 * Integer.MAX_VALUE));

    public static Coach getCoachSample1() {
        return new Coach().id(1L).nome("nome1").cognome("cognome1");
    }

    public static Coach getCoachSample2() {
        return new Coach().id(2L).nome("nome2").cognome("cognome2");
    }

    public static Coach getCoachRandomSampleGenerator() {
        return new Coach().id(longCount.incrementAndGet()).nome(UUID.randomUUID().toString()).cognome(UUID.randomUUID().toString());
    }
}
