package it.costantino.astaapp.service.dto;

import jakarta.validation.constraints.*;
import java.io.Serializable;
import java.util.Objects;

/**
 * A DTO for the {@link it.costantino.astaapp.domain.Squadra} entity.
 */
@SuppressWarnings("common-java:DuplicatedBlocks")
public class SquadraDTO implements Serializable {

    private Long id;

    @NotNull
    private String nome;

    private LegaDTO lega;

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

    public LegaDTO getLega() {
        return lega;
    }

    public void setLega(LegaDTO lega) {
        this.lega = lega;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof SquadraDTO)) {
            return false;
        }

        SquadraDTO squadraDTO = (SquadraDTO) o;
        if (this.id == null) {
            return false;
        }
        return Objects.equals(this.id, squadraDTO.id);
    }

    @Override
    public int hashCode() {
        return Objects.hash(this.id);
    }

    // prettier-ignore
    @Override
    public String toString() {
        return "SquadraDTO{" +
            "id=" + getId() +
            ", nome='" + getNome() + "'" +
            ", lega=" + getLega() +
            "}";
    }
}
