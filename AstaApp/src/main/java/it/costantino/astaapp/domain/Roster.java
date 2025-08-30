package it.costantino.astaapp.domain;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import jakarta.persistence.*;
import jakarta.validation.constraints.*;
import java.io.Serializable;

/**
 * A Roster.
 */
@Entity
@Table(name = "roster")
@SuppressWarnings("common-java:DuplicatedBlocks")
public class Roster implements Serializable {

    private static final long serialVersionUID = 1L;

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Column(name = "id")
    private Long id;

    @NotNull
    @Column(name = "full", nullable = false)
    private Boolean full;

    @NotNull
    @Column(name = "port", nullable = false)
    private Long port;

    @NotNull
    @Column(name = "dif", nullable = false)
    private Long dif;

    @NotNull
    @Column(name = "cc", nullable = false)
    private Long cc;

    @NotNull
    @Column(name = "att", nullable = false)
    private Long att;

    @ManyToOne(fetch = FetchType.LAZY)
    @JsonIgnoreProperties(value = { "coaches", "giocatores", "rosters", "watchLists", "lega" }, allowSetters = true)
    private Squadra squadra;

    // jhipster-needle-entity-add-field - JHipster will add fields here

    public Long getId() {
        return this.id;
    }

    public Roster id(Long id) {
        this.setId(id);
        return this;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public Boolean getFull() {
        return this.full;
    }

    public Roster full(Boolean full) {
        this.setFull(full);
        return this;
    }

    public void setFull(Boolean full) {
        this.full = full;
    }

    public Long getPort() {
        return this.port;
    }

    public Roster port(Long port) {
        this.setPort(port);
        return this;
    }

    public void setPort(Long port) {
        this.port = port;
    }

    public Long getDif() {
        return this.dif;
    }

    public Roster dif(Long dif) {
        this.setDif(dif);
        return this;
    }

    public void setDif(Long dif) {
        this.dif = dif;
    }

    public Long getCc() {
        return this.cc;
    }

    public Roster cc(Long cc) {
        this.setCc(cc);
        return this;
    }

    public void setCc(Long cc) {
        this.cc = cc;
    }

    public Long getAtt() {
        return this.att;
    }

    public Roster att(Long att) {
        this.setAtt(att);
        return this;
    }

    public void setAtt(Long att) {
        this.att = att;
    }

    public Squadra getSquadra() {
        return this.squadra;
    }

    public void setSquadra(Squadra squadra) {
        this.squadra = squadra;
    }

    public Roster squadra(Squadra squadra) {
        this.setSquadra(squadra);
        return this;
    }

    // jhipster-needle-entity-add-getters-setters - JHipster will add getters and setters here

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof Roster)) {
            return false;
        }
        return getId() != null && getId().equals(((Roster) o).getId());
    }

    @Override
    public int hashCode() {
        // see https://vladmihalcea.com/how-to-implement-equals-and-hashcode-using-the-jpa-entity-identifier/
        return getClass().hashCode();
    }

    // prettier-ignore
    @Override
    public String toString() {
        return "Roster{" +
            "id=" + getId() +
            ", full='" + getFull() + "'" +
            ", port=" + getPort() +
            ", dif=" + getDif() +
            ", cc=" + getCc() +
            ", att=" + getAtt() +
            "}";
    }
}
