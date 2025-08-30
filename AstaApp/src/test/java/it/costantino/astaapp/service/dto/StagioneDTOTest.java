package it.costantino.astaapp.service.dto;

import static org.assertj.core.api.Assertions.assertThat;

import it.costantino.astaapp.web.rest.TestUtil;
import org.junit.jupiter.api.Test;

class StagioneDTOTest {

    @Test
    void dtoEqualsVerifier() throws Exception {
        TestUtil.equalsVerifier(StagioneDTO.class);
        StagioneDTO stagioneDTO1 = new StagioneDTO();
        stagioneDTO1.setId(1L);
        StagioneDTO stagioneDTO2 = new StagioneDTO();
        assertThat(stagioneDTO1).isNotEqualTo(stagioneDTO2);
        stagioneDTO2.setId(stagioneDTO1.getId());
        assertThat(stagioneDTO1).isEqualTo(stagioneDTO2);
        stagioneDTO2.setId(2L);
        assertThat(stagioneDTO1).isNotEqualTo(stagioneDTO2);
        stagioneDTO1.setId(null);
        assertThat(stagioneDTO1).isNotEqualTo(stagioneDTO2);
    }
}
