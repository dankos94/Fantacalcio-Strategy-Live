package it.costantino.astaapp.domain;

import static it.costantino.astaapp.domain.GiocatoreTestSamples.*;
import static it.costantino.astaapp.domain.SquadraTestSamples.*;
import static org.assertj.core.api.Assertions.assertThat;

import it.costantino.astaapp.web.rest.TestUtil;
import org.junit.jupiter.api.Test;

class GiocatoreTest {

    @Test
    void equalsVerifier() throws Exception {
        TestUtil.equalsVerifier(Giocatore.class);
        Giocatore giocatore1 = getGiocatoreSample1();
        Giocatore giocatore2 = new Giocatore();
        assertThat(giocatore1).isNotEqualTo(giocatore2);

        giocatore2.setId(giocatore1.getId());
        assertThat(giocatore1).isEqualTo(giocatore2);

        giocatore2 = getGiocatoreSample2();
        assertThat(giocatore1).isNotEqualTo(giocatore2);
    }

    @Test
    void squadraTest() {
        Giocatore giocatore = getGiocatoreRandomSampleGenerator();
        Squadra squadraBack = getSquadraRandomSampleGenerator();

        giocatore.setSquadra(squadraBack);
        assertThat(giocatore.getSquadra()).isEqualTo(squadraBack);

        giocatore.squadra(null);
        assertThat(giocatore.getSquadra()).isNull();
    }
}
