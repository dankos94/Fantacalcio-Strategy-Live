package it.costantino.astaapp.domain;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import jakarta.persistence.*;
import jakarta.validation.constraints.*;
import java.io.Serializable;
import java.util.HashSet;
import java.util.Set;

/**
 * A Squadra.
 */
@Entity
@Table(name = "squadra")
@SuppressWarnings("common-java:DuplicatedBlocks")
public class Squadra implements Serializable {

    private static final long serialVersionUID = 1L;

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Column(name = "id")
    private Long id;

    @NotNull
    @Column(name = "nome", nullable = false)
    private String nome;

    @OneToMany(fetch = FetchType.LAZY, mappedBy = "squadra")
    @JsonIgnoreProperties(value = { "squadra" }, allowSetters = true)
    private Set<Coach> coaches = new HashSet<>();

    @OneToMany(fetch = FetchType.LAZY, mappedBy = "squadra")
    @JsonIgnoreProperties(value = { "squadra", "watchList" }, allowSetters = true)
    private Set<Giocatore> giocatores = new HashSet<>();

    @OneToMany(fetch = FetchType.LAZY, mappedBy = "squadra")
    @JsonIgnoreProperties(value = { "squadra" }, allowSetters = true)
    private Set<Roster> rosters = new HashSet<>();

    @OneToMany(fetch = FetchType.LAZY, mappedBy = "squadra")
    @JsonIgnoreProperties(value = { "giocatores", "squadra" }, allowSetters = true)
    private Set<WatchList> watchLists = new HashSet<>();

    @ManyToOne(fetch = FetchType.LAZY)
    @JsonIgnoreProperties(value = { "squadras", "stagione" }, allowSetters = true)
    private Lega lega;

    // jhipster-needle-entity-add-field - JHipster will add fields here

    public Long getId() {
        return this.id;
    }

    public Squadra id(Long id) {
        this.setId(id);
        return this;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public String getNome() {
        return this.nome;
    }

    public Squadra nome(String nome) {
        this.setNome(nome);
        return this;
    }

    public void setNome(String nome) {
        this.nome = nome;
    }

    public Set<Coach> getCoaches() {
        return this.coaches;
    }

    public void setCoaches(Set<Coach> coaches) {
        if (this.coaches != null) {
            this.coaches.forEach(i -> i.setSquadra(null));
        }
        if (coaches != null) {
            coaches.forEach(i -> i.setSquadra(this));
        }
        this.coaches = coaches;
    }

    public Squadra coaches(Set<Coach> coaches) {
        this.setCoaches(coaches);
        return this;
    }

    public Squadra addCoach(Coach coach) {
        this.coaches.add(coach);
        coach.setSquadra(this);
        return this;
    }

    public Squadra removeCoach(Coach coach) {
        this.coaches.remove(coach);
        coach.setSquadra(null);
        return this;
    }

    public Set<Giocatore> getGiocatores() {
        return this.giocatores;
    }

    public void setGiocatores(Set<Giocatore> giocatores) {
        if (this.giocatores != null) {
            this.giocatores.forEach(i -> i.setSquadra(null));
        }
        if (giocatores != null) {
            giocatores.forEach(i -> i.setSquadra(this));
        }
        this.giocatores = giocatores;
    }

    public Squadra giocatores(Set<Giocatore> giocatores) {
        this.setGiocatores(giocatores);
        return this;
    }

    public Squadra addGiocatore(Giocatore giocatore) {
        this.giocatores.add(giocatore);
        giocatore.setSquadra(this);
        return this;
    }

    public Squadra removeGiocatore(Giocatore giocatore) {
        this.giocatores.remove(giocatore);
        giocatore.setSquadra(null);
        return this;
    }

    public Set<Roster> getRosters() {
        return this.rosters;
    }

    public void setRosters(Set<Roster> rosters) {
        if (this.rosters != null) {
            this.rosters.forEach(i -> i.setSquadra(null));
        }
        if (rosters != null) {
            rosters.forEach(i -> i.setSquadra(this));
        }
        this.rosters = rosters;
    }

    public Squadra rosters(Set<Roster> rosters) {
        this.setRosters(rosters);
        return this;
    }

    public Squadra addRoster(Roster roster) {
        this.rosters.add(roster);
        roster.setSquadra(this);
        return this;
    }

    public Squadra removeRoster(Roster roster) {
        this.rosters.remove(roster);
        roster.setSquadra(null);
        return this;
    }

    public Set<WatchList> getWatchLists() {
        return this.watchLists;
    }

    public void setWatchLists(Set<WatchList> watchLists) {
        if (this.watchLists != null) {
            this.watchLists.forEach(i -> i.setSquadra(null));
        }
        if (watchLists != null) {
            watchLists.forEach(i -> i.setSquadra(this));
        }
        this.watchLists = watchLists;
    }

    public Squadra watchLists(Set<WatchList> watchLists) {
        this.setWatchLists(watchLists);
        return this;
    }

    public Squadra addWatchList(WatchList watchList) {
        this.watchLists.add(watchList);
        watchList.setSquadra(this);
        return this;
    }

    public Squadra removeWatchList(WatchList watchList) {
        this.watchLists.remove(watchList);
        watchList.setSquadra(null);
        return this;
    }

    public Lega getLega() {
        return this.lega;
    }

    public void setLega(Lega lega) {
        this.lega = lega;
    }

    public Squadra lega(Lega lega) {
        this.setLega(lega);
        return this;
    }

    // jhipster-needle-entity-add-getters-setters - JHipster will add getters and setters here

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof Squadra)) {
            return false;
        }
        return getId() != null && getId().equals(((Squadra) o).getId());
    }

    @Override
    public int hashCode() {
        // see https://vladmihalcea.com/how-to-implement-equals-and-hashcode-using-the-jpa-entity-identifier/
        return getClass().hashCode();
    }

    // prettier-ignore
    @Override
    public String toString() {
        return "Squadra{" +
            "id=" + getId() +
            ", nome='" + getNome() + "'" +
            "}";
    }
}
