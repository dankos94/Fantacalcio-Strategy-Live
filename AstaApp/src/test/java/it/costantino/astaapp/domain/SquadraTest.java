package it.costantino.astaapp.domain;

import static it.costantino.astaapp.domain.CoachTestSamples.*;
import static it.costantino.astaapp.domain.GiocatoreTestSamples.*;
import static it.costantino.astaapp.domain.LegaTestSamples.*;
import static it.costantino.astaapp.domain.RosterTestSamples.*;
import static it.costantino.astaapp.domain.SquadraTestSamples.*;
import static it.costantino.astaapp.domain.WatchListTestSamples.*;
import static org.assertj.core.api.Assertions.assertThat;

import it.costantino.astaapp.web.rest.TestUtil;
import java.util.HashSet;
import java.util.Set;
import org.junit.jupiter.api.Test;

class SquadraTest {

    @Test
    void equalsVerifier() throws Exception {
        TestUtil.equalsVerifier(Squadra.class);
        Squadra squadra1 = getSquadraSample1();
        Squadra squadra2 = new Squadra();
        assertThat(squadra1).isNotEqualTo(squadra2);

        squadra2.setId(squadra1.getId());
        assertThat(squadra1).isEqualTo(squadra2);

        squadra2 = getSquadraSample2();
        assertThat(squadra1).isNotEqualTo(squadra2);
    }

    @Test
    void coachTest() {
        Squadra squadra = getSquadraRandomSampleGenerator();
        Coach coachBack = getCoachRandomSampleGenerator();

        squadra.addCoach(coachBack);
        assertThat(squadra.getCoaches()).containsOnly(coachBack);
        assertThat(coachBack.getSquadra()).isEqualTo(squadra);

        squadra.removeCoach(coachBack);
        assertThat(squadra.getCoaches()).doesNotContain(coachBack);
        assertThat(coachBack.getSquadra()).isNull();

        squadra.coaches(new HashSet<>(Set.of(coachBack)));
        assertThat(squadra.getCoaches()).containsOnly(coachBack);
        assertThat(coachBack.getSquadra()).isEqualTo(squadra);

        squadra.setCoaches(new HashSet<>());
        assertThat(squadra.getCoaches()).doesNotContain(coachBack);
        assertThat(coachBack.getSquadra()).isNull();
    }

    @Test
    void giocatoreTest() {
        Squadra squadra = getSquadraRandomSampleGenerator();
        Giocatore giocatoreBack = getGiocatoreRandomSampleGenerator();

        squadra.addGiocatore(giocatoreBack);
        assertThat(squadra.getGiocatores()).containsOnly(giocatoreBack);
        assertThat(giocatoreBack.getSquadra()).isEqualTo(squadra);

        squadra.removeGiocatore(giocatoreBack);
        assertThat(squadra.getGiocatores()).doesNotContain(giocatoreBack);
        assertThat(giocatoreBack.getSquadra()).isNull();

        squadra.giocatores(new HashSet<>(Set.of(giocatoreBack)));
        assertThat(squadra.getGiocatores()).containsOnly(giocatoreBack);
        assertThat(giocatoreBack.getSquadra()).isEqualTo(squadra);

        squadra.setGiocatores(new HashSet<>());
        assertThat(squadra.getGiocatores()).doesNotContain(giocatoreBack);
        assertThat(giocatoreBack.getSquadra()).isNull();
    }

    @Test
    void rosterTest() {
        Squadra squadra = getSquadraRandomSampleGenerator();
        Roster rosterBack = getRosterRandomSampleGenerator();

        squadra.addRoster(rosterBack);
        assertThat(squadra.getRosters()).containsOnly(rosterBack);
        assertThat(rosterBack.getSquadra()).isEqualTo(squadra);

        squadra.removeRoster(rosterBack);
        assertThat(squadra.getRosters()).doesNotContain(rosterBack);
        assertThat(rosterBack.getSquadra()).isNull();

        squadra.rosters(new HashSet<>(Set.of(rosterBack)));
        assertThat(squadra.getRosters()).containsOnly(rosterBack);
        assertThat(rosterBack.getSquadra()).isEqualTo(squadra);

        squadra.setRosters(new HashSet<>());
        assertThat(squadra.getRosters()).doesNotContain(rosterBack);
        assertThat(rosterBack.getSquadra()).isNull();
    }

    @Test
    void watchListTest() {
        Squadra squadra = getSquadraRandomSampleGenerator();
        WatchList watchListBack = getWatchListRandomSampleGenerator();

        squadra.addWatchList(watchListBack);
        assertThat(squadra.getWatchLists()).containsOnly(watchListBack);
        assertThat(watchListBack.getSquadra()).isEqualTo(squadra);

        squadra.removeWatchList(watchListBack);
        assertThat(squadra.getWatchLists()).doesNotContain(watchListBack);
        assertThat(watchListBack.getSquadra()).isNull();

        squadra.watchLists(new HashSet<>(Set.of(watchListBack)));
        assertThat(squadra.getWatchLists()).containsOnly(watchListBack);
        assertThat(watchListBack.getSquadra()).isEqualTo(squadra);

        squadra.setWatchLists(new HashSet<>());
        assertThat(squadra.getWatchLists()).doesNotContain(watchListBack);
        assertThat(watchListBack.getSquadra()).isNull();
    }

    @Test
    void legaTest() {
        Squadra squadra = getSquadraRandomSampleGenerator();
        Lega legaBack = getLegaRandomSampleGenerator();

        squadra.setLega(legaBack);
        assertThat(squadra.getLega()).isEqualTo(legaBack);

        squadra.lega(null);
        assertThat(squadra.getLega()).isNull();
    }
}
