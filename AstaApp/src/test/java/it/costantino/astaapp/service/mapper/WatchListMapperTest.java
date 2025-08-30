package it.costantino.astaapp.service.mapper;

import static it.costantino.astaapp.domain.WatchListAsserts.*;
import static it.costantino.astaapp.domain.WatchListTestSamples.*;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class WatchListMapperTest {

    private WatchListMapper watchListMapper;

    @BeforeEach
    void setUp() {
        watchListMapper = new WatchListMapperImpl();
    }

    @Test
    void shouldConvertToDtoAndBack() {
        var expected = getWatchListSample1();
        var actual = watchListMapper.toEntity(watchListMapper.toDto(expected));
        assertWatchListAllPropertiesEquals(expected, actual);
    }
}
