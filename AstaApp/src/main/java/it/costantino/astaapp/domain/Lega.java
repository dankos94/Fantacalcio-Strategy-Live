package it.costantino.astaapp.domain;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import jakarta.persistence.*;
import jakarta.validation.constraints.*;
import java.io.Serializable;
import java.util.HashSet;
import java.util.Set;

/**
 * A Lega.
 */
@Entity
@Table(name = "lega")
@SuppressWarnings("common-java:DuplicatedBlocks")
public class Lega implements Serializable {

    private static final long serialVersionUID = 1L;

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Column(name = "id")
    private Long id;

    @NotNull
    @Column(name = "nome", nullable = false)
    private String nome;

    @NotNull
    @Column(name = "budget", nullable = false)
    private Long budget;

    @OneToMany(fetch = FetchType.LAZY, mappedBy = "lega")
    @JsonIgnoreProperties(value = { "coaches", "giocatores", "rosters", "watchLists", "lega" }, allowSetters = true)
    private Set<Squadra> squadras = new HashSet<>();

    @ManyToOne(fetch = FetchType.LAZY)
    @JsonIgnoreProperties(value = { "legas" }, allowSetters = true)
    private Stagione stagione;

    // jhipster-needle-entity-add-field - JHipster will add fields here

    public Long getId() {
        return this.id;
    }

    public Lega id(Long id) {
        this.setId(id);
        return this;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public String getNome() {
        return this.nome;
    }

    public Lega nome(String nome) {
        this.setNome(nome);
        return this;
    }

    public void setNome(String nome) {
        this.nome = nome;
    }

    public Long getBudget() {
        return this.budget;
    }

    public Lega budget(Long budget) {
        this.setBudget(budget);
        return this;
    }

    public void setBudget(Long budget) {
        this.budget = budget;
    }

    public Set<Squadra> getSquadras() {
        return this.squadras;
    }

    public void setSquadras(Set<Squadra> squadras) {
        if (this.squadras != null) {
            this.squadras.forEach(i -> i.setLega(null));
        }
        if (squadras != null) {
            squadras.forEach(i -> i.setLega(this));
        }
        this.squadras = squadras;
    }

    public Lega squadras(Set<Squadra> squadras) {
        this.setSquadras(squadras);
        return this;
    }

    public Lega addSquadra(Squadra squadra) {
        this.squadras.add(squadra);
        squadra.setLega(this);
        return this;
    }

    public Lega removeSquadra(Squadra squadra) {
        this.squadras.remove(squadra);
        squadra.setLega(null);
        return this;
    }

    public Stagione getStagione() {
        return this.stagione;
    }

    public void setStagione(Stagione stagione) {
        this.stagione = stagione;
    }

    public Lega stagione(Stagione stagione) {
        this.setStagione(stagione);
        return this;
    }

    // jhipster-needle-entity-add-getters-setters - JHipster will add getters and setters here

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof Lega)) {
            return false;
        }
        return getId() != null && getId().equals(((Lega) o).getId());
    }

    @Override
    public int hashCode() {
        // see https://vladmihalcea.com/how-to-implement-equals-and-hashcode-using-the-jpa-entity-identifier/
        return getClass().hashCode();
    }

    // prettier-ignore
    @Override
    public String toString() {
        return "Lega{" +
            "id=" + getId() +
            ", nome='" + getNome() + "'" +
            ", budget=" + getBudget() +
            "}";
    }
}
