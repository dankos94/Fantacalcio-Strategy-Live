package it.costantino.astaapp.service.dto;

import static org.assertj.core.api.Assertions.assertThat;

import it.costantino.astaapp.web.rest.TestUtil;
import org.junit.jupiter.api.Test;

class GiocatoreDTOTest {

    @Test
    void dtoEqualsVerifier() throws Exception {
        TestUtil.equalsVerifier(GiocatoreDTO.class);
        GiocatoreDTO giocatoreDTO1 = new GiocatoreDTO();
        giocatoreDTO1.setId(1L);
        GiocatoreDTO giocatoreDTO2 = new GiocatoreDTO();
        assertThat(giocatoreDTO1).isNotEqualTo(giocatoreDTO2);
        giocatoreDTO2.setId(giocatoreDTO1.getId());
        assertThat(giocatoreDTO1).isEqualTo(giocatoreDTO2);
        giocatoreDTO2.setId(2L);
        assertThat(giocatoreDTO1).isNotEqualTo(giocatoreDTO2);
        giocatoreDTO1.setId(null);
        assertThat(giocatoreDTO1).isNotEqualTo(giocatoreDTO2);
    }
}
