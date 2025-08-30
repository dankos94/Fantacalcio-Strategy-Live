package it.costantino.astaapp.service.mapper;

import static it.costantino.astaapp.domain.LegaAsserts.*;
import static it.costantino.astaapp.domain.LegaTestSamples.*;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class LegaMapperTest {

    private LegaMapper legaMapper;

    @BeforeEach
    void setUp() {
        legaMapper = new LegaMapperImpl();
    }

    @Test
    void shouldConvertToDtoAndBack() {
        var expected = getLegaSample1();
        var actual = legaMapper.toEntity(legaMapper.toDto(expected));
        assertLegaAllPropertiesEquals(expected, actual);
    }
}
