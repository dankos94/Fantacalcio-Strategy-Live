package it.costantino.astaapp.service.dto;

import jakarta.validation.constraints.*;
import java.io.Serializable;
import java.util.Objects;

/**
 * A DTO for the {@link it.costantino.astaapp.domain.Stagione} entity.
 */
@SuppressWarnings("common-java:DuplicatedBlocks")
public class StagioneDTO implements Serializable {

    private Long id;

    @NotNull
    private String nome;

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

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof StagioneDTO)) {
            return false;
        }

        StagioneDTO stagioneDTO = (StagioneDTO) o;
        if (this.id == null) {
            return false;
        }
        return Objects.equals(this.id, stagioneDTO.id);
    }

    @Override
    public int hashCode() {
        return Objects.hash(this.id);
    }

    // prettier-ignore
    @Override
    public String toString() {
        return "StagioneDTO{" +
            "id=" + getId() +
            ", nome='" + getNome() + "'" +
            "}";
    }
}
