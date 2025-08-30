package it.costantino.astaapp.service.mapper;

import static it.costantino.astaapp.domain.RosterAsserts.*;
import static it.costantino.astaapp.domain.RosterTestSamples.*;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class RosterMapperTest {

    private RosterMapper rosterMapper;

    @BeforeEach
    void setUp() {
        rosterMapper = new RosterMapperImpl();
    }

    @Test
    void shouldConvertToDtoAndBack() {
        var expected = getRosterSample1();
        var actual = rosterMapper.toEntity(rosterMapper.toDto(expected));
        assertRosterAllPropertiesEquals(expected, actual);
    }
}
