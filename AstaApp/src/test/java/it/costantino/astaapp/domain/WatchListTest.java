package it.costantino.astaapp.domain;

import static it.costantino.astaapp.domain.SquadraTestSamples.*;
import static it.costantino.astaapp.domain.WatchListTestSamples.*;
import static org.assertj.core.api.Assertions.assertThat;

import it.costantino.astaapp.web.rest.TestUtil;
import org.junit.jupiter.api.Test;

class WatchListTest {

    @Test
    void equalsVerifier() throws Exception {
        TestUtil.equalsVerifier(WatchList.class);
        WatchList watchList1 = getWatchListSample1();
        WatchList watchList2 = new WatchList();
        assertThat(watchList1).isNotEqualTo(watchList2);

        watchList2.setId(watchList1.getId());
        assertThat(watchList1).isEqualTo(watchList2);

        watchList2 = getWatchListSample2();
        assertThat(watchList1).isNotEqualTo(watchList2);
    }

    @Test
    void squadraTest() {
        WatchList watchList = getWatchListRandomSampleGenerator();
        Squadra squadraBack = getSquadraRandomSampleGenerator();

        watchList.setSquadra(squadraBack);
        assertThat(watchList.getSquadra()).isEqualTo(squadraBack);

        watchList.squadra(null);
        assertThat(watchList.getSquadra()).isNull();
    }
}
