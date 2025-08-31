package it.costantino.astaapp.domain;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import it.costantino.astaapp.domain.enumeration.Role;
import jakarta.persistence.*;
import java.io.Serializable;

/**
 * A Giocatore.
 */
@Entity
@Table(name = "giocatore")
@SuppressWarnings("common-java:DuplicatedBlocks")
public class Giocatore implements Serializable {

    private static final long serialVersionUID = 1L;

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Column(name = "id")
    private Long id;

    @Column(name = "player")
    private String player;

    @Column(name = "team")
    private String team;

    @Column(name = "season")
    private String season;

    @Column(name = "age")
    private String age;

    @Column(name = "squad")
    private String squad;

    @Column(name = "comp")
    private String comp;

    @Column(name = "country")
    private String country;

    @Column(name = "mp")
    private Long mp;

    @Column(name = "starts")
    private Long starts;

    @Column(name = "starts_pct")
    private Long startsPct;

    @Column(name = "minutes")
    private Long minutes;

    @Column(name = "nins")
    private Long nins;

    @Column(name = "gls")
    private Long gls;

    @Column(name = "ast")
    private Long ast;

    @Column(name = "g_plus_a")
    private Long gPlusA;

    @Column(name = "g_pk")
    private Long gPk;

    @Column(name = "pk")
    private Long pk;

    @Column(name = "pkatt")
    private Long pkatt;

    @Column(name = "crdy")
    private Long crdy;

    @Column(name = "crdr")
    private Long crdr;

    @Column(name = "xg")
    private Long xg;

    @Column(name = "xag")
    private Long xag;

    @Column(name = "npxg")
    private Long npxg;

    @Column(name = "xg_plus_xag")
    private Long xgPlusXag;

    @Column(name = "npxg_plus_xag")
    private Long npxgPlusXag;

    @Column(name = "prgp")
    private Long prgp;

    @Column(name = "prgc")
    private Long prgc;

    @Column(name = "prgr")
    private Long prgr;

    @Column(name = "gls_per_90")
    private Long glsPer90;

    @Column(name = "ast_per_90")
    private Long astPer90;

    @Column(name = "g_plus_a_per_90")
    private Long gPlusAPer90;

    @Column(name = "xg_per_90")
    private Long xgPer90;

    @Column(name = "xag_per_90")
    private Long xagPer90;

    @Column(name = "xg_plus_xag_per_90")
    private Long xgPlusXagPer90;

    @Column(name = "xg_diff")
    private Long xgDiff;

    @Column(name = "xa_diff")
    private Long xaDiff;

    @Column(name = "trend")
    private String trend;

    @Column(name = "newleague")
    private String newleague;

    @Column(name = "sourcefile")
    private String sourcefile;

    @Enumerated(EnumType.STRING)
    @Column(name = "role")
    private Role role;

    @Column(name = "career_mp")
    private Long careerMp;

    @Column(name = "career_starts")
    private Long careerStarts;

    @Column(name = "career_min")
    private Long careerMin;

    @Column(name = "career_90_s")
    private Long career90s;

    @Column(name = "career_gls")
    private Long careerGls;

    @Column(name = "career_ast")
    private Long careerAst;

    @Column(name = "career_g_plus_a")
    private Long careerGPlusA;

    @Column(name = "career_xg")
    private Long careerXg;

    @Column(name = "career_xag")
    private Long careerXag;

    @Column(name = "career_xg_plus_xag")
    private Long careerXgPlusXag;

    @Column(name = "career_npxg")
    private Long careerNpxg;

    @Column(name = "career_npxg_plus_xag")
    private Long careerNpxgPlusXag;

    @Column(name = "career_gls_per_90")
    private Long careerGlsPer90;

    @Column(name = "career_ast_per_90")
    private Long careerAstPer90;

    @Column(name = "career_g_plus_a_per_90")
    private Long careerGPlusAPer90;

    @Column(name = "career_xg_per_90")
    private Long careerXgPer90;

    @Column(name = "career_xag_per_90")
    private Long careerXagPer90;

    @Column(name = "career_xg_plus_xag_per_90")
    private Long careerXgPlusXagPer90;

    @ManyToOne(fetch = FetchType.LAZY)
    @JsonIgnoreProperties(value = { "coaches", "giocatores", "rosters", "watchLists", "lega" }, allowSetters = true)
    private Squadra squadra;

    @ManyToOne(fetch = FetchType.LAZY)
    @JsonIgnoreProperties(value = { "giocatores", "squadra" }, allowSetters = true)
    private WatchList watchList;

    // jhipster-needle-entity-add-field - JHipster will add fields here

    public Long getId() {
        return this.id;
    }

    public Giocatore id(Long id) {
        this.setId(id);
        return this;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public String getPlayer() {
        return this.player;
    }

    public Giocatore player(String player) {
        this.setPlayer(player);
        return this;
    }

    public void setPlayer(String player) {
        this.player = player;
    }

    public String getTeam() {
        return this.team;
    }

    public Giocatore team(String team) {
        this.setTeam(team);
        return this;
    }

    public void setTeam(String team) {
        this.team = team;
    }

    public String getSeason() {
        return this.season;
    }

    public Giocatore season(String season) {
        this.setSeason(season);
        return this;
    }

    public void setSeason(String season) {
        this.season = season;
    }

    public String getAge() {
        return this.age;
    }

    public Giocatore age(String age) {
        this.setAge(age);
        return this;
    }

    public void setAge(String age) {
        this.age = age;
    }

    public String getSquad() {
        return this.squad;
    }

    public Giocatore squad(String squad) {
        this.setSquad(squad);
        return this;
    }

    public void setSquad(String squad) {
        this.squad = squad;
    }

    public String getComp() {
        return this.comp;
    }

    public Giocatore comp(String comp) {
        this.setComp(comp);
        return this;
    }

    public void setComp(String comp) {
        this.comp = comp;
    }

    public String getCountry() {
        return this.country;
    }

    public Giocatore country(String country) {
        this.setCountry(country);
        return this;
    }

    public void setCountry(String country) {
        this.country = country;
    }

    public Long getMp() {
        return this.mp;
    }

    public Giocatore mp(Long mp) {
        this.setMp(mp);
        return this;
    }

    public void setMp(Long mp) {
        this.mp = mp;
    }

    public Long getStarts() {
        return this.starts;
    }

    public Giocatore starts(Long starts) {
        this.setStarts(starts);
        return this;
    }

    public void setStarts(Long starts) {
        this.starts = starts;
    }

    public Long getStartsPct() {
        return this.startsPct;
    }

    public Giocatore startsPct(Long startsPct) {
        this.setStartsPct(startsPct);
        return this;
    }

    public void setStartsPct(Long startsPct) {
        this.startsPct = startsPct;
    }

    public Long getMinutes() {
        return this.minutes;
    }

    public Giocatore minutes(Long minutes) {
        this.setMinutes(minutes);
        return this;
    }

    public void setMinutes(Long minutes) {
        this.minutes = minutes;
    }

    public Long getNins() {
        return this.nins;
    }

    public Giocatore nins(Long nins) {
        this.setNins(nins);
        return this;
    }

    public void setNins(Long nins) {
        this.nins = nins;
    }

    public Long getGls() {
        return this.gls;
    }

    public Giocatore gls(Long gls) {
        this.setGls(gls);
        return this;
    }

    public void setGls(Long gls) {
        this.gls = gls;
    }

    public Long getAst() {
        return this.ast;
    }

    public Giocatore ast(Long ast) {
        this.setAst(ast);
        return this;
    }

    public void setAst(Long ast) {
        this.ast = ast;
    }

    public Long getgPlusA() {
        return this.gPlusA;
    }

    public Giocatore gPlusA(Long gPlusA) {
        this.setgPlusA(gPlusA);
        return this;
    }

    public void setgPlusA(Long gPlusA) {
        this.gPlusA = gPlusA;
    }

    public Long getgPk() {
        return this.gPk;
    }

    public Giocatore gPk(Long gPk) {
        this.setgPk(gPk);
        return this;
    }

    public void setgPk(Long gPk) {
        this.gPk = gPk;
    }

    public Long getPk() {
        return this.pk;
    }

    public Giocatore pk(Long pk) {
        this.setPk(pk);
        return this;
    }

    public void setPk(Long pk) {
        this.pk = pk;
    }

    public Long getPkatt() {
        return this.pkatt;
    }

    public Giocatore pkatt(Long pkatt) {
        this.setPkatt(pkatt);
        return this;
    }

    public void setPkatt(Long pkatt) {
        this.pkatt = pkatt;
    }

    public Long getCrdy() {
        return this.crdy;
    }

    public Giocatore crdy(Long crdy) {
        this.setCrdy(crdy);
        return this;
    }

    public void setCrdy(Long crdy) {
        this.crdy = crdy;
    }

    public Long getCrdr() {
        return this.crdr;
    }

    public Giocatore crdr(Long crdr) {
        this.setCrdr(crdr);
        return this;
    }

    public void setCrdr(Long crdr) {
        this.crdr = crdr;
    }

    public Long getXg() {
        return this.xg;
    }

    public Giocatore xg(Long xg) {
        this.setXg(xg);
        return this;
    }

    public void setXg(Long xg) {
        this.xg = xg;
    }

    public Long getXag() {
        return this.xag;
    }

    public Giocatore xag(Long xag) {
        this.setXag(xag);
        return this;
    }

    public void setXag(Long xag) {
        this.xag = xag;
    }

    public Long getNpxg() {
        return this.npxg;
    }

    public Giocatore npxg(Long npxg) {
        this.setNpxg(npxg);
        return this;
    }

    public void setNpxg(Long npxg) {
        this.npxg = npxg;
    }

    public Long getXgPlusXag() {
        return this.xgPlusXag;
    }

    public Giocatore xgPlusXag(Long xgPlusXag) {
        this.setXgPlusXag(xgPlusXag);
        return this;
    }

    public void setXgPlusXag(Long xgPlusXag) {
        this.xgPlusXag = xgPlusXag;
    }

    public Long getNpxgPlusXag() {
        return this.npxgPlusXag;
    }

    public Giocatore npxgPlusXag(Long npxgPlusXag) {
        this.setNpxgPlusXag(npxgPlusXag);
        return this;
    }

    public void setNpxgPlusXag(Long npxgPlusXag) {
        this.npxgPlusXag = npxgPlusXag;
    }

    public Long getPrgp() {
        return this.prgp;
    }

    public Giocatore prgp(Long prgp) {
        this.setPrgp(prgp);
        return this;
    }

    public void setPrgp(Long prgp) {
        this.prgp = prgp;
    }

    public Long getPrgc() {
        return this.prgc;
    }

    public Giocatore prgc(Long prgc) {
        this.setPrgc(prgc);
        return this;
    }

    public void setPrgc(Long prgc) {
        this.prgc = prgc;
    }

    public Long getPrgr() {
        return this.prgr;
    }

    public Giocatore prgr(Long prgr) {
        this.setPrgr(prgr);
        return this;
    }

    public void setPrgr(Long prgr) {
        this.prgr = prgr;
    }

    public Long getGlsPer90() {
        return this.glsPer90;
    }

    public Giocatore glsPer90(Long glsPer90) {
        this.setGlsPer90(glsPer90);
        return this;
    }

    public void setGlsPer90(Long glsPer90) {
        this.glsPer90 = glsPer90;
    }

    public Long getAstPer90() {
        return this.astPer90;
    }

    public Giocatore astPer90(Long astPer90) {
        this.setAstPer90(astPer90);
        return this;
    }

    public void setAstPer90(Long astPer90) {
        this.astPer90 = astPer90;
    }

    public Long getgPlusAPer90() {
        return this.gPlusAPer90;
    }

    public Giocatore gPlusAPer90(Long gPlusAPer90) {
        this.setgPlusAPer90(gPlusAPer90);
        return this;
    }

    public void setgPlusAPer90(Long gPlusAPer90) {
        this.gPlusAPer90 = gPlusAPer90;
    }

    public Long getXgPer90() {
        return this.xgPer90;
    }

    public Giocatore xgPer90(Long xgPer90) {
        this.setXgPer90(xgPer90);
        return this;
    }

    public void setXgPer90(Long xgPer90) {
        this.xgPer90 = xgPer90;
    }

    public Long getXagPer90() {
        return this.xagPer90;
    }

    public Giocatore xagPer90(Long xagPer90) {
        this.setXagPer90(xagPer90);
        return this;
    }

    public void setXagPer90(Long xagPer90) {
        this.xagPer90 = xagPer90;
    }

    public Long getXgPlusXagPer90() {
        return this.xgPlusXagPer90;
    }

    public Giocatore xgPlusXagPer90(Long xgPlusXagPer90) {
        this.setXgPlusXagPer90(xgPlusXagPer90);
        return this;
    }

    public void setXgPlusXagPer90(Long xgPlusXagPer90) {
        this.xgPlusXagPer90 = xgPlusXagPer90;
    }

    public Long getXgDiff() {
        return this.xgDiff;
    }

    public Giocatore xgDiff(Long xgDiff) {
        this.setXgDiff(xgDiff);
        return this;
    }

    public void setXgDiff(Long xgDiff) {
        this.xgDiff = xgDiff;
    }

    public Long getXaDiff() {
        return this.xaDiff;
    }

    public Giocatore xaDiff(Long xaDiff) {
        this.setXaDiff(xaDiff);
        return this;
    }

    public void setXaDiff(Long xaDiff) {
        this.xaDiff = xaDiff;
    }

    public String getTrend() {
        return this.trend;
    }

    public Giocatore trend(String trend) {
        this.setTrend(trend);
        return this;
    }

    public void setTrend(String trend) {
        this.trend = trend;
    }

    public String getNewleague() {
        return this.newleague;
    }

    public Giocatore newleague(String newleague) {
        this.setNewleague(newleague);
        return this;
    }

    public void setNewleague(String newleague) {
        this.newleague = newleague;
    }

    public String getSourcefile() {
        return this.sourcefile;
    }

    public Giocatore sourcefile(String sourcefile) {
        this.setSourcefile(sourcefile);
        return this;
    }

    public void setSourcefile(String sourcefile) {
        this.sourcefile = sourcefile;
    }

    public Role getRole() {
        return this.role;
    }

    public Giocatore role(Role role) {
        this.setRole(role);
        return this;
    }

    public void setRole(Role role) {
        this.role = role;
    }

    public Long getCareerMp() {
        return this.careerMp;
    }

    public Giocatore careerMp(Long careerMp) {
        this.setCareerMp(careerMp);
        return this;
    }

    public void setCareerMp(Long careerMp) {
        this.careerMp = careerMp;
    }

    public Long getCareerStarts() {
        return this.careerStarts;
    }

    public Giocatore careerStarts(Long careerStarts) {
        this.setCareerStarts(careerStarts);
        return this;
    }

    public void setCareerStarts(Long careerStarts) {
        this.careerStarts = careerStarts;
    }

    public Long getCareerMin() {
        return this.careerMin;
    }

    public Giocatore careerMin(Long careerMin) {
        this.setCareerMin(careerMin);
        return this;
    }

    public void setCareerMin(Long careerMin) {
        this.careerMin = careerMin;
    }

    public Long getCareer90s() {
        return this.career90s;
    }

    public Giocatore career90s(Long career90s) {
        this.setCareer90s(career90s);
        return this;
    }

    public void setCareer90s(Long career90s) {
        this.career90s = career90s;
    }

    public Long getCareerGls() {
        return this.careerGls;
    }

    public Giocatore careerGls(Long careerGls) {
        this.setCareerGls(careerGls);
        return this;
    }

    public void setCareerGls(Long careerGls) {
        this.careerGls = careerGls;
    }

    public Long getCareerAst() {
        return this.careerAst;
    }

    public Giocatore careerAst(Long careerAst) {
        this.setCareerAst(careerAst);
        return this;
    }

    public void setCareerAst(Long careerAst) {
        this.careerAst = careerAst;
    }

    public Long getCareerGPlusA() {
        return this.careerGPlusA;
    }

    public Giocatore careerGPlusA(Long careerGPlusA) {
        this.setCareerGPlusA(careerGPlusA);
        return this;
    }

    public void setCareerGPlusA(Long careerGPlusA) {
        this.careerGPlusA = careerGPlusA;
    }

    public Long getCareerXg() {
        return this.careerXg;
    }

    public Giocatore careerXg(Long careerXg) {
        this.setCareerXg(careerXg);
        return this;
    }

    public void setCareerXg(Long careerXg) {
        this.careerXg = careerXg;
    }

    public Long getCareerXag() {
        return this.careerXag;
    }

    public Giocatore careerXag(Long careerXag) {
        this.setCareerXag(careerXag);
        return this;
    }

    public void setCareerXag(Long careerXag) {
        this.careerXag = careerXag;
    }

    public Long getCareerXgPlusXag() {
        return this.careerXgPlusXag;
    }

    public Giocatore careerXgPlusXag(Long careerXgPlusXag) {
        this.setCareerXgPlusXag(careerXgPlusXag);
        return this;
    }

    public void setCareerXgPlusXag(Long careerXgPlusXag) {
        this.careerXgPlusXag = careerXgPlusXag;
    }

    public Long getCareerNpxg() {
        return this.careerNpxg;
    }

    public Giocatore careerNpxg(Long careerNpxg) {
        this.setCareerNpxg(careerNpxg);
        return this;
    }

    public void setCareerNpxg(Long careerNpxg) {
        this.careerNpxg = careerNpxg;
    }

    public Long getCareerNpxgPlusXag() {
        return this.careerNpxgPlusXag;
    }

    public Giocatore careerNpxgPlusXag(Long careerNpxgPlusXag) {
        this.setCareerNpxgPlusXag(careerNpxgPlusXag);
        return this;
    }

    public void setCareerNpxgPlusXag(Long careerNpxgPlusXag) {
        this.careerNpxgPlusXag = careerNpxgPlusXag;
    }

    public Long getCareerGlsPer90() {
        return this.careerGlsPer90;
    }

    public Giocatore careerGlsPer90(Long careerGlsPer90) {
        this.setCareerGlsPer90(careerGlsPer90);
        return this;
    }

    public void setCareerGlsPer90(Long careerGlsPer90) {
        this.careerGlsPer90 = careerGlsPer90;
    }

    public Long getCareerAstPer90() {
        return this.careerAstPer90;
    }

    public Giocatore careerAstPer90(Long careerAstPer90) {
        this.setCareerAstPer90(careerAstPer90);
        return this;
    }

    public void setCareerAstPer90(Long careerAstPer90) {
        this.careerAstPer90 = careerAstPer90;
    }

    public Long getCareerGPlusAPer90() {
        return this.careerGPlusAPer90;
    }

    public Giocatore careerGPlusAPer90(Long careerGPlusAPer90) {
        this.setCareerGPlusAPer90(careerGPlusAPer90);
        return this;
    }

    public void setCareerGPlusAPer90(Long careerGPlusAPer90) {
        this.careerGPlusAPer90 = careerGPlusAPer90;
    }

    public Long getCareerXgPer90() {
        return this.careerXgPer90;
    }

    public Giocatore careerXgPer90(Long careerXgPer90) {
        this.setCareerXgPer90(careerXgPer90);
        return this;
    }

    public void setCareerXgPer90(Long careerXgPer90) {
        this.careerXgPer90 = careerXgPer90;
    }

    public Long getCareerXagPer90() {
        return this.careerXagPer90;
    }

    public Giocatore careerXagPer90(Long careerXagPer90) {
        this.setCareerXagPer90(careerXagPer90);
        return this;
    }

    public void setCareerXagPer90(Long careerXagPer90) {
        this.careerXagPer90 = careerXagPer90;
    }

    public Long getCareerXgPlusXagPer90() {
        return this.careerXgPlusXagPer90;
    }

    public Giocatore careerXgPlusXagPer90(Long careerXgPlusXagPer90) {
        this.setCareerXgPlusXagPer90(careerXgPlusXagPer90);
        return this;
    }

    public void setCareerXgPlusXagPer90(Long careerXgPlusXagPer90) {
        this.careerXgPlusXagPer90 = careerXgPlusXagPer90;
    }

    public Squadra getSquadra() {
        return this.squadra;
    }

    public void setSquadra(Squadra squadra) {
        this.squadra = squadra;
    }

    public Giocatore squadra(Squadra squadra) {
        this.setSquadra(squadra);
        return this;
    }

    public WatchList getWatchList() {
        return this.watchList;
    }

    public void setWatchList(WatchList watchList) {
        this.watchList = watchList;
    }

    public Giocatore watchList(WatchList watchList) {
        this.setWatchList(watchList);
        return this;
    }

    // jhipster-needle-entity-add-getters-setters - JHipster will add getters and setters here

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof Giocatore)) {
            return false;
        }
        return getId() != null && getId().equals(((Giocatore) o).getId());
    }

    @Override
    public int hashCode() {
        // see https://vladmihalcea.com/how-to-implement-equals-and-hashcode-using-the-jpa-entity-identifier/
        return getClass().hashCode();
    }

    // prettier-ignore
    @Override
    public String toString() {
        return "Giocatore{" +
            "id=" + getId() +
            ", player='" + getPlayer() + "'" +
            ", team='" + getTeam() + "'" +
            ", season='" + getSeason() + "'" +
            ", age='" + getAge() + "'" +
            ", squad='" + getSquad() + "'" +
            ", comp='" + getComp() + "'" +
            ", country='" + getCountry() + "'" +
            ", mp=" + getMp() +
            ", starts=" + getStarts() +
            ", startsPct=" + getStartsPct() +
            ", minutes=" + getMinutes() +
            ", nins=" + getNins() +
            ", gls=" + getGls() +
            ", ast=" + getAst() +
            ", gPlusA=" + getgPlusA() +
            ", gPk=" + getgPk() +
            ", pk=" + getPk() +
            ", pkatt=" + getPkatt() +
            ", crdy=" + getCrdy() +
            ", crdr=" + getCrdr() +
            ", xg=" + getXg() +
            ", xag=" + getXag() +
            ", npxg=" + getNpxg() +
            ", xgPlusXag=" + getXgPlusXag() +
            ", npxgPlusXag=" + getNpxgPlusXag() +
            ", prgp=" + getPrgp() +
            ", prgc=" + getPrgc() +
            ", prgr=" + getPrgr() +
            ", glsPer90=" + getGlsPer90() +
            ", astPer90=" + getAstPer90() +
            ", gPlusAPer90=" + getgPlusAPer90() +
            ", xgPer90=" + getXgPer90() +
            ", xagPer90=" + getXagPer90() +
            ", xgPlusXagPer90=" + getXgPlusXagPer90() +
            ", xgDiff=" + getXgDiff() +
            ", xaDiff=" + getXaDiff() +
            ", trend='" + getTrend() + "'" +
            ", newleague='" + getNewleague() + "'" +
            ", sourcefile='" + getSourcefile() + "'" +
            ", role='" + getRole() + "'" +
            ", careerMp=" + getCareerMp() +
            ", careerStarts=" + getCareerStarts() +
            ", careerMin=" + getCareerMin() +
            ", career90s=" + getCareer90s() +
            ", careerGls=" + getCareerGls() +
            ", careerAst=" + getCareerAst() +
            ", careerGPlusA=" + getCareerGPlusA() +
            ", careerXg=" + getCareerXg() +
            ", careerXag=" + getCareerXag() +
            ", careerXgPlusXag=" + getCareerXgPlusXag() +
            ", careerNpxg=" + getCareerNpxg() +
            ", careerNpxgPlusXag=" + getCareerNpxgPlusXag() +
            ", careerGlsPer90=" + getCareerGlsPer90() +
            ", careerAstPer90=" + getCareerAstPer90() +
            ", careerGPlusAPer90=" + getCareerGPlusAPer90() +
            ", careerXgPer90=" + getCareerXgPer90() +
            ", careerXagPer90=" + getCareerXagPer90() +
            ", careerXgPlusXagPer90=" + getCareerXgPlusXagPer90() +
            "}";
    }
}
