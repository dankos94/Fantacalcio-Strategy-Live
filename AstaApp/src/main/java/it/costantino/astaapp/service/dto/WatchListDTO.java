package it.costantino.astaapp.service.dto;

import java.io.Serializable;
import java.time.Instant;
import java.util.Objects;

/**
 * A DTO for the {@link it.costantino.astaapp.domain.WatchList} entity.
 */
@SuppressWarnings("common-java:DuplicatedBlocks")
public class WatchListDTO implements Serializable {

    private Long id;

    private Instant date;

    private String version;

    private SquadraDTO squadra;

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public Instant getDate() {
        return date;
    }

    public void setDate(Instant date) {
        this.date = date;
    }

    public String getVersion() {
        return version;
    }

    public void setVersion(String version) {
        this.version = version;
    }

    public SquadraDTO getSquadra() {
        return squadra;
    }

    public void setSquadra(SquadraDTO squadra) {
        this.squadra = squadra;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof WatchListDTO)) {
            return false;
        }

        WatchListDTO watchListDTO = (WatchListDTO) o;
        if (this.id == null) {
            return false;
        }
        return Objects.equals(this.id, watchListDTO.id);
    }

    @Override
    public int hashCode() {
        return Objects.hash(this.id);
    }

    // prettier-ignore
    @Override
    public String toString() {
        return "WatchListDTO{" +
            "id=" + getId() +
            ", date='" + getDate() + "'" +
            ", version='" + getVersion() + "'" +
            ", squadra=" + getSquadra() +
            "}";
    }
}
