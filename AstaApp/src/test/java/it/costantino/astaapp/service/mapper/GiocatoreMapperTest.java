package it.costantino.astaapp.service.mapper;

import static it.costantino.astaapp.domain.GiocatoreAsserts.*;
import static it.costantino.astaapp.domain.GiocatoreTestSamples.*;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class GiocatoreMapperTest {

    private GiocatoreMapper giocatoreMapper;

    @BeforeEach
    void setUp() {
        giocatoreMapper = new GiocatoreMapperImpl();
    }

    @Test
    void shouldConvertToDtoAndBack() {
        var expected = getGiocatoreSample1();
        var actual = giocatoreMapper.toEntity(giocatoreMapper.toDto(expected));
        assertGiocatoreAllPropertiesEquals(expected, actual);
    }
}
