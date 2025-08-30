package it.costantino.astaapp.service.mapper;

import static it.costantino.astaapp.domain.SquadraAsserts.*;
import static it.costantino.astaapp.domain.SquadraTestSamples.*;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class SquadraMapperTest {

    private SquadraMapper squadraMapper;

    @BeforeEach
    void setUp() {
        squadraMapper = new SquadraMapperImpl();
    }

    @Test
    void shouldConvertToDtoAndBack() {
        var expected = getSquadraSample1();
        var actual = squadraMapper.toEntity(squadraMapper.toDto(expected));
        assertSquadraAllPropertiesEquals(expected, actual);
    }
}
