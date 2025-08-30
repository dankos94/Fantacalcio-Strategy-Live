package it.costantino.astaapp.service.dto;

import jakarta.validation.constraints.*;
import java.io.Serializable;
import java.util.Objects;

/**
 * A DTO for the {@link it.costantino.astaapp.domain.Lega} entity.
 */
@SuppressWarnings("common-java:DuplicatedBlocks")
public class LegaDTO implements Serializable {

    private Long id;

    @NotNull
    private String nome;

    @NotNull
    private Long budget;

    private StagioneDTO stagione;

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

    public Long getBudget() {
        return budget;
    }

    public void setBudget(Long budget) {
        this.budget = budget;
    }

    public StagioneDTO getStagione() {
        return stagione;
    }

    public void setStagione(StagioneDTO stagione) {
        this.stagione = stagione;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof LegaDTO)) {
            return false;
        }

        LegaDTO legaDTO = (LegaDTO) o;
        if (this.id == null) {
            return false;
        }
        return Objects.equals(this.id, legaDTO.id);
    }

    @Override
    public int hashCode() {
        return Objects.hash(this.id);
    }

    // prettier-ignore
    @Override
    public String toString() {
        return "LegaDTO{" +
            "id=" + getId() +
            ", nome='" + getNome() + "'" +
            ", budget=" + getBudget() +
            ", stagione=" + getStagione() +
            "}";
    }
}
