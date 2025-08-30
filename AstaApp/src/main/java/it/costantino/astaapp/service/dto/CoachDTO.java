package it.costantino.astaapp.service.dto;

import jakarta.validation.constraints.*;
import java.io.Serializable;
import java.util.Objects;

/**
 * A DTO for the {@link it.costantino.astaapp.domain.Coach} entity.
 */
@SuppressWarnings("common-java:DuplicatedBlocks")
public class CoachDTO implements Serializable {

    private Long id;

    @NotNull
    private String nome;

    private String cognome;

    private Boolean itsMe;

    private SquadraDTO squadra;

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public String getNome() {
        return nome;
    }

    public void setNome(String nome) {
        this.nome = nome;
    }

    public String getCognome() {
        return cognome;
    }

    public void setCognome(String cognome) {
        this.cognome = cognome;
    }

    public Boolean getItsMe() {
        return itsMe;
    }

    public void setItsMe(Boolean itsMe) {
        this.itsMe = itsMe;
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
        if (!(o instanceof CoachDTO)) {
            return false;
        }

        CoachDTO coachDTO = (CoachDTO) o;
        if (this.id == null) {
            return false;
        }
        return Objects.equals(this.id, coachDTO.id);
    }

    @Override
    public int hashCode() {
        return Objects.hash(this.id);
    }

    // prettier-ignore
    @Override
    public String toString() {
        return "CoachDTO{" +
            "id=" + getId() +
            ", nome='" + getNome() + "'" +
            ", cognome='" + getCognome() + "'" +
            ", itsMe='" + getItsMe() + "'" +
            ", squadra=" + getSquadra() +
            "}";
    }
}
