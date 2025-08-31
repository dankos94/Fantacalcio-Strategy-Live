package it.costantino.astaapp.domain;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import jakarta.persistence.*;
import java.io.Serializable;
import java.time.Instant;
import java.util.HashSet;
import java.util.Set;

/**
 * A WatchList.
 */
@Entity
@Table(name = "watch_list")
@SuppressWarnings("common-java:DuplicatedBlocks")
public class WatchList implements Serializable {

    private static final long serialVersionUID = 1L;

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Column(name = "id")
    private Long id;

    @Column(name = "date")
    private Instant date;

    @Column(name = "version")
    private String version;

    @OneToMany(fetch = FetchType.LAZY, mappedBy = "watchList")
    @JsonIgnoreProperties(value = { "squadra", "watchList" }, allowSetters = true)
    private Set<Giocatore> giocatores = new HashSet<>();

    @ManyToOne(fetch = FetchType.LAZY)
    @JsonIgnoreProperties(value = { "coaches", "giocatores", "rosters", "watchLists", "lega" }, allowSetters = true)
    private Squadra squadra;

    // jhipster-needle-entity-add-field - JHipster will add fields here

    public Long getId() {
        return this.id;
    }

    public WatchList id(Long id) {
        this.setId(id);
        return this;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public Instant getDate() {
        return this.date;
    }

    public WatchList date(Instant date) {
        this.setDate(date);
        return this;
    }

    public void setDate(Instant date) {
        this.date = date;
    }

    public String getVersion() {
        return this.version;
    }

    public WatchList version(String version) {
        this.setVersion(version);
        return this;
    }

    public void setVersion(String version) {
        this.version = version;
    }

    public Set<Giocatore> getGiocatores() {
        return this.giocatores;
    }

    public void setGiocatores(Set<Giocatore> giocatores) {
        if (this.giocatores != null) {
            this.giocatores.forEach(i -> i.setWatchList(null));
        }
        if (giocatores != null) {
            giocatores.forEach(i -> i.setWatchList(this));
        }
        this.giocatores = giocatores;
    }

    public WatchList giocatores(Set<Giocatore> giocatores) {
        this.setGiocatores(giocatores);
        return this;
    }

    public WatchList addGiocatore(Giocatore giocatore) {
        this.giocatores.add(giocatore);
        giocatore.setWatchList(this);
        return this;
    }

    public WatchList removeGiocatore(Giocatore giocatore) {
        this.giocatores.remove(giocatore);
        giocatore.setWatchList(null);
        return this;
    }

    public Squadra getSquadra() {
        return this.squadra;
    }

    public void setSquadra(Squadra squadra) {
        this.squadra = squadra;
    }

    public WatchList squadra(Squadra squadra) {
        this.setSquadra(squadra);
        return this;
    }

    // jhipster-needle-entity-add-getters-setters - JHipster will add getters and setters here

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof WatchList)) {
            return false;
        }
        return getId() != null && getId().equals(((WatchList) o).getId());
    }

    @Override
    public int hashCode() {
        // see https://vladmihalcea.com/how-to-implement-equals-and-hashcode-using-the-jpa-entity-identifier/
        return getClass().hashCode();
    }

    // prettier-ignore
    @Override
    public String toString() {
        return "WatchList{" +
            "id=" + getId() +
            ", date='" + getDate() + "'" +
            ", version='" + getVersion() + "'" +
            "}";
    }
}
