package it.costantino.astaapp.domain;

import static it.costantino.astaapp.domain.RosterTestSamples.*;
import static it.costantino.astaapp.domain.SquadraTestSamples.*;
import static org.assertj.core.api.Assertions.assertThat;

import it.costantino.astaapp.web.rest.TestUtil;
import org.junit.jupiter.api.Test;

class RosterTest {

    @Test
    void equalsVerifier() throws Exception {
        TestUtil.equalsVerifier(Roster.class);
        Roster roster1 = getRosterSample1();
        Roster roster2 = new Roster();
        assertThat(roster1).isNotEqualTo(roster2);

        roster2.setId(roster1.getId());
        assertThat(roster1).isEqualTo(roster2);

        roster2 = getRosterSample2();
        assertThat(roster1).isNotEqualTo(roster2);
    }

    @Test
    void squadraTest() {
        Roster roster = getRosterRandomSampleGenerator();
        Squadra squadraBack = getSquadraRandomSampleGenerator();

        roster.setSquadra(squadraBack);
        assertThat(roster.getSquadra()).isEqualTo(squadraBack);

        roster.squadra(null);
        assertThat(roster.getSquadra()).isNull();
    }
}
