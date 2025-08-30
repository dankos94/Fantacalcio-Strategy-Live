package it.costantino.astaapp.service.dto;

import jakarta.validation.constraints.*;
import java.io.Serializable;
import java.util.Objects;

/**
 * A DTO for the {@link it.costantino.astaapp.domain.Roster} entity.
 */
@SuppressWarnings("common-java:DuplicatedBlocks")
public class RosterDTO implements Serializable {

    private Long id;

    @NotNull
    private Boolean full;

    @NotNull
    private Long port;

    @NotNull
    private Long dif;

    @NotNull
    private Long cc;

    @NotNull
    private Long att;

    private SquadraDTO squadra;

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public Boolean getFull() {
        return full;
    }

    public void setFull(Boolean full) {
        this.full = full;
    }

    public Long getPort() {
        return port;
    }

    public void setPort(Long port) {
        this.port = port;
    }

    public Long getDif() {
        return dif;
    }

    public void setDif(Long dif) {
        this.dif = dif;
    }

    public Long getCc() {
        return cc;
    }

    public void setCc(Long cc) {
        this.cc = cc;
    }

    public Long getAtt() {
        return att;
    }

    public void setAtt(Long att) {
        this.att = att;
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
        if (!(o instanceof RosterDTO)) {
            return false;
        }

        RosterDTO rosterDTO = (RosterDTO) o;
        if (this.id == null) {
            return false;
        }
        return Objects.equals(this.id, rosterDTO.id);
    }

    @Override
    public int hashCode() {
        return Objects.hash(this.id);
    }

    // prettier-ignore
    @Override
    public String toString() {
        return "RosterDTO{" +
            "id=" + getId() +
            ", full='" + getFull() + "'" +
            ", port=" + getPort() +
            ", dif=" + getDif() +
            ", cc=" + getCc() +
            ", att=" + getAtt() +
            ", squadra=" + getSquadra() +
            "}";
    }
}
