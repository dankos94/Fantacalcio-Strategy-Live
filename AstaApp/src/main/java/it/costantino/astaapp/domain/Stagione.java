package it.costantino.astaapp.domain;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import jakarta.persistence.*;
import jakarta.validation.constraints.*;
import java.io.Serializable;
import java.util.HashSet;
import java.util.Set;

/**
 * A Stagione.
 */
@Entity
@Table(name = "stagione")
@SuppressWarnings("common-java:DuplicatedBlocks")
public class Stagione implements Serializable {

    private static final long serialVersionUID = 1L;

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Column(name = "id")
    private Long id;

    @NotNull
    @Column(name = "nome", nullable = false)
    private String nome;

    @OneToMany(fetch = FetchType.LAZY, mappedBy = "stagione")
    @JsonIgnoreProperties(value = { "squadras", "stagione" }, allowSetters = true)
    private Set<Lega> legas = new HashSet<>();

    // jhipster-needle-entity-add-field - JHipster will add fields here

    public Long getId() {
        return this.id;
    }

    public Stagione id(Long id) {
        this.setId(id);
        return this;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public String getNome() {
        return this.nome;
    }

    public Stagione nome(String nome) {
        this.setNome(nome);
        return this;
    }

    public void setNome(String nome) {
        this.nome = nome;
    }

    public Set<Lega> getLegas() {
        return this.legas;
    }

    public void setLegas(Set<Lega> legas) {
        if (this.legas != null) {
            this.legas.forEach(i -> i.setStagione(null));
        }
        if (legas != null) {
            legas.forEach(i -> i.setStagione(this));
        }
        this.legas = legas;
    }

    public Stagione legas(Set<Lega> legas) {
        this.setLegas(legas);
        return this;
    }

    public Stagione addLega(Lega lega) {
        this.legas.add(lega);
        lega.setStagione(this);
        return this;
    }

    public Stagione removeLega(Lega lega) {
        this.legas.remove(lega);
        lega.setStagione(null);
        return this;
    }

    // jhipster-needle-entity-add-getters-setters - JHipster will add getters and setters here

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof Stagione)) {
            return false;
        }
        return getId() != null && getId().equals(((Stagione) o).getId());
    }

    @Override
    public int hashCode() {
        // see https://vladmihalcea.com/how-to-implement-equals-and-hashcode-using-the-jpa-entity-identifier/
        return getClass().hashCode();
    }

    // prettier-ignore
    @Override
    public String toString() {
        return "Stagione{" +
            "id=" + getId() +
            ", nome='" + getNome() + "'" +
            "}";
    }
}
