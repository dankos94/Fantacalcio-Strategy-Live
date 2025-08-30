package it.costantino.astaapp.domain;

import java.util.Random;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

public class WatchListTestSamples {

    private static final Random random = new Random();
    private static final AtomicLong longCount = new AtomicLong(random.nextInt() + (2 * Integer.MAX_VALUE));

    public static WatchList getWatchListSample1() {
        return new WatchList().id(1L).version("version1");
    }

    public static WatchList getWatchListSample2() {
        return new WatchList().id(2L).version("version2");
    }

    public static WatchList getWatchListRandomSampleGenerator() {
        return new WatchList().id(longCount.incrementAndGet()).version(UUID.randomUUID().toString());
    }
}
