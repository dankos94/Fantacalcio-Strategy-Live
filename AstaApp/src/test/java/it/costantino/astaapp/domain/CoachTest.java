package it.costantino.astaapp.domain;

import static it.costantino.astaapp.domain.CoachTestSamples.*;
import static it.costantino.astaapp.domain.SquadraTestSamples.*;
import static org.assertj.core.api.Assertions.assertThat;

import it.costantino.astaapp.web.rest.TestUtil;
import org.junit.jupiter.api.Test;

class CoachTest {

    @Test
    void equalsVerifier() throws Exception {
        TestUtil.equalsVerifier(Coach.class);
        Coach coach1 = getCoachSample1();
        Coach coach2 = new Coach();
        assertThat(coach1).isNotEqualTo(coach2);

        coach2.setId(coach1.getId());
        assertThat(coach1).isEqualTo(coach2);

        coach2 = getCoachSample2();
        assertThat(coach1).isNotEqualTo(coach2);
    }

    @Test
    void squadraTest() {
        Coach coach = getCoachRandomSampleGenerator();
        Squadra squadraBack = getSquadraRandomSampleGenerator();

        coach.setSquadra(squadraBack);
        assertThat(coach.getSquadra()).isEqualTo(squadraBack);

        coach.squadra(null);
        assertThat(coach.getSquadra()).isNull();
    }
}
