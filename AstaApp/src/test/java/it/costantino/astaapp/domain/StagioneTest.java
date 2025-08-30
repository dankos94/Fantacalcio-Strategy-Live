package it.costantino.astaapp.domain;

import static it.costantino.astaapp.domain.LegaTestSamples.*;
import static it.costantino.astaapp.domain.StagioneTestSamples.*;
import static org.assertj.core.api.Assertions.assertThat;

import it.costantino.astaapp.web.rest.TestUtil;
import java.util.HashSet;
import java.util.Set;
import org.junit.jupiter.api.Test;

class StagioneTest {

    @Test
    void equalsVerifier() throws Exception {
        TestUtil.equalsVerifier(Stagione.class);
        Stagione stagione1 = getStagioneSample1();
        Stagione stagione2 = new Stagione();
        assertThat(stagione1).isNotEqualTo(stagione2);

        stagione2.setId(stagione1.getId());
        assertThat(stagione1).isEqualTo(stagione2);

        stagione2 = getStagioneSample2();
        assertThat(stagione1).isNotEqualTo(stagione2);
    }

    @Test
    void legaTest() {
        Stagione stagione = getStagioneRandomSampleGenerator();
        Lega legaBack = getLegaRandomSampleGenerator();

        stagione.addLega(legaBack);
        assertThat(stagione.getLegas()).containsOnly(legaBack);
        assertThat(legaBack.getStagione()).isEqualTo(stagione);

        stagione.removeLega(legaBack);
        assertThat(stagione.getLegas()).doesNotContain(legaBack);
        assertThat(legaBack.getStagione()).isNull();

        stagione.legas(new HashSet<>(Set.of(legaBack)));
        assertThat(stagione.getLegas()).containsOnly(legaBack);
        assertThat(legaBack.getStagione()).isEqualTo(stagione);

        stagione.setLegas(new HashSet<>());
        assertThat(stagione.getLegas()).doesNotContain(legaBack);
        assertThat(legaBack.getStagione()).isNull();
    }
}
