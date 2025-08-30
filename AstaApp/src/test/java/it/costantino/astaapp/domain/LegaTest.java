package it.costantino.astaapp.domain;

import static it.costantino.astaapp.domain.LegaTestSamples.*;
import static it.costantino.astaapp.domain.SquadraTestSamples.*;
import static it.costantino.astaapp.domain.StagioneTestSamples.*;
import static org.assertj.core.api.Assertions.assertThat;

import it.costantino.astaapp.web.rest.TestUtil;
import java.util.HashSet;
import java.util.Set;
import org.junit.jupiter.api.Test;

class LegaTest {

    @Test
    void equalsVerifier() throws Exception {
        TestUtil.equalsVerifier(Lega.class);
        Lega lega1 = getLegaSample1();
        Lega lega2 = new Lega();
        assertThat(lega1).isNotEqualTo(lega2);

        lega2.setId(lega1.getId());
        assertThat(lega1).isEqualTo(lega2);

        lega2 = getLegaSample2();
        assertThat(lega1).isNotEqualTo(lega2);
    }

    @Test
    void squadraTest() {
        Lega lega = getLegaRandomSampleGenerator();
        Squadra squadraBack = getSquadraRandomSampleGenerator();

        lega.addSquadra(squadraBack);
        assertThat(lega.getSquadras()).containsOnly(squadraBack);
        assertThat(squadraBack.getLega()).isEqualTo(lega);

        lega.removeSquadra(squadraBack);
        assertThat(lega.getSquadras()).doesNotContain(squadraBack);
        assertThat(squadraBack.getLega()).isNull();

        lega.squadras(new HashSet<>(Set.of(squadraBack)));
        assertThat(lega.getSquadras()).containsOnly(squadraBack);
        assertThat(squadraBack.getLega()).isEqualTo(lega);

        lega.setSquadras(new HashSet<>());
        assertThat(lega.getSquadras()).doesNotContain(squadraBack);
        assertThat(squadraBack.getLega()).isNull();
    }

    @Test
    void stagioneTest() {
        Lega lega = getLegaRandomSampleGenerator();
        Stagione stagioneBack = getStagioneRandomSampleGenerator();

        lega.setStagione(stagioneBack);
        assertThat(lega.getStagione()).isEqualTo(stagioneBack);

        lega.stagione(null);
        assertThat(lega.getStagione()).isNull();
    }
}
