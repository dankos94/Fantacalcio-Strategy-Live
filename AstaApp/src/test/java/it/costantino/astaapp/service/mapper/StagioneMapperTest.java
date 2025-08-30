package it.costantino.astaapp.service.mapper;

import static it.costantino.astaapp.domain.StagioneAsserts.*;
import static it.costantino.astaapp.domain.StagioneTestSamples.*;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class StagioneMapperTest {

    private StagioneMapper stagioneMapper;

    @BeforeEach
    void setUp() {
        stagioneMapper = new StagioneMapperImpl();
    }

    @Test
    void shouldConvertToDtoAndBack() {
        var expected = getStagioneSample1();
        var actual = stagioneMapper.toEntity(stagioneMapper.toDto(expected));
        assertStagioneAllPropertiesEquals(expected, actual);
    }
}
