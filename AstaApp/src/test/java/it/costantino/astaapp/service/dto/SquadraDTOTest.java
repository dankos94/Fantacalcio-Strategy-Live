package it.costantino.astaapp.service.dto;

import static org.assertj.core.api.Assertions.assertThat;

import it.costantino.astaapp.web.rest.TestUtil;
import org.junit.jupiter.api.Test;

class SquadraDTOTest {

    @Test
    void dtoEqualsVerifier() throws Exception {
        TestUtil.equalsVerifier(SquadraDTO.class);
        SquadraDTO squadraDTO1 = new SquadraDTO();
        squadraDTO1.setId(1L);
        SquadraDTO squadraDTO2 = new SquadraDTO();
        assertThat(squadraDTO1).isNotEqualTo(squadraDTO2);
        squadraDTO2.setId(squadraDTO1.getId());
        assertThat(squadraDTO1).isEqualTo(squadraDTO2);
        squadraDTO2.setId(2L);
        assertThat(squadraDTO1).isNotEqualTo(squadraDTO2);
        squadraDTO1.setId(null);
        assertThat(squadraDTO1).isNotEqualTo(squadraDTO2);
    }
}
