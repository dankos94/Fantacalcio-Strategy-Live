package it.costantino.astaapp.service.dto;

import static org.assertj.core.api.Assertions.assertThat;

import it.costantino.astaapp.web.rest.TestUtil;
import org.junit.jupiter.api.Test;

class LegaDTOTest {

    @Test
    void dtoEqualsVerifier() throws Exception {
        TestUtil.equalsVerifier(LegaDTO.class);
        LegaDTO legaDTO1 = new LegaDTO();
        legaDTO1.setId(1L);
        LegaDTO legaDTO2 = new LegaDTO();
        assertThat(legaDTO1).isNotEqualTo(legaDTO2);
        legaDTO2.setId(legaDTO1.getId());
        assertThat(legaDTO1).isEqualTo(legaDTO2);
        legaDTO2.setId(2L);
        assertThat(legaDTO1).isNotEqualTo(legaDTO2);
        legaDTO1.setId(null);
        assertThat(legaDTO1).isNotEqualTo(legaDTO2);
    }
}
