package it.costantino.astaapp.service.dto;

import it.costantino.astaapp.domain.enumeration.Role;
import java.io.Serializable;
import java.util.Objects;

/**
 * A DTO for the {@link it.costantino.astaapp.domain.Giocatore} entity.
 */
@SuppressWarnings("common-java:DuplicatedBlocks")
public class GiocatoreDTO implements Serializable {

    private Long id;

    private String player;

    private String team;

    private String season;

    private String age;

    private String squad;

    private String comp;

    private String country;

    private Long mp;

    private Long starts;

    private Long startsPct;

    private Long minutes;

    private Long nins;

    private Long gls;

    private Long ast;

    private Long gPlusA;

    private Long gPk;

    private Long pk;

    private Long pkatt;

    private Long crdy;

    private Long crdr;

    private Long xg;

    private Long xag;

    private Long npxg;

    private Long xgPlusXag;

    private Long npxgPlusXag;

    private Long prgp;

    private Long prgc;

    private Long prgr;

    private Long glsPer90;

    private Long astPer90;

    private Long gPlusAPer90;

    private Long xgPer90;

    private Long xagPer90;

    private Long xgPlusXagPer90;

    private Long xgDiff;

    private Long xaDiff;

    private String trend;

    private String newleague;

    private String sourcefile;

    private Role role;

    private Long careerMp;

    private Long careerStarts;

    private Long careerMin;

    private Long career90s;

    private Long careerGls;

    private Long careerAst;

    private Long careerGPlusA;

    private Long careerXg;

    private Long careerXag;

    private Long careerXgPlusXag;

    private Long careerNpxg;

    private Long careerNpxgPlusXag;

    private Long careerGlsPer90;

    private Long careerAstPer90;

    private Long careerGPlusAPer90;

    private Long careerXgPer90;

    private Long careerXagPer90;

    private Long careerXgPlusXagPer90;

    private SquadraDTO squadra;

    private WatchListDTO watchList;

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public String getPlayer() {
        return player;
    }

    public void setPlayer(String player) {
        this.player = player;
    }

    public String getTeam() {
        return team;
    }

    public void setTeam(String team) {
        this.team = team;
    }

    public String getSeason() {
        return season;
    }

    public void setSeason(String season) {
        this.season = season;
    }

    public String getAge() {
        return age;
    }

    public void setAge(String age) {
        this.age = age;
    }

    public String getSquad() {
        return squad;
    }

    public void setSquad(String squad) {
        this.squad = squad;
    }

    public String getComp() {
        return comp;
    }

    public void setComp(String comp) {
        this.comp = comp;
    }

    public String getCountry() {
        return country;
    }

    public void setCountry(String country) {
        this.country = country;
    }

    public Long getMp() {
        return mp;
    }

    public void setMp(Long mp) {
        this.mp = mp;
    }

    public Long getStarts() {
        return starts;
    }

    public void setStarts(Long starts) {
        this.starts = starts;
    }

    public Long getStartsPct() {
        return startsPct;
    }

    public void setStartsPct(Long startsPct) {
        this.startsPct = startsPct;
    }

    public Long getMinutes() {
        return minutes;
    }

    public void setMinutes(Long minutes) {
        this.minutes = minutes;
    }

    public Long getNins() {
        return nins;
    }

    public void setNins(Long nins) {
        this.nins = nins;
    }

    public Long getGls() {
        return gls;
    }

    public void setGls(Long gls) {
        this.gls = gls;
    }

    public Long getAst() {
        return ast;
    }

    public void setAst(Long ast) {
        this.ast = ast;
    }

    public Long getgPlusA() {
        return gPlusA;
    }

    public void setgPlusA(Long gPlusA) {
        this.gPlusA = gPlusA;
    }

    public Long getgPk() {
        return gPk;
    }

    public void setgPk(Long gPk) {
        this.gPk = gPk;
    }

    public Long getPk() {
        return pk;
    }

    public void setPk(Long pk) {
        this.pk = pk;
    }

    public Long getPkatt() {
        return pkatt;
    }

    public void setPkatt(Long pkatt) {
        this.pkatt = pkatt;
    }

    public Long getCrdy() {
        return crdy;
    }

    public void setCrdy(Long crdy) {
        this.crdy = crdy;
    }

    public Long getCrdr() {
        return crdr;
    }

    public void setCrdr(Long crdr) {
        this.crdr = crdr;
    }

    public Long getXg() {
        return xg;
    }

    public void setXg(Long xg) {
        this.xg = xg;
    }

    public Long getXag() {
        return xag;
    }

    public void setXag(Long xag) {
        this.xag = xag;
    }

    public Long getNpxg() {
        return npxg;
    }

    public void setNpxg(Long npxg) {
        this.npxg = npxg;
    }

    public Long getXgPlusXag() {
        return xgPlusXag;
    }

    public void setXgPlusXag(Long xgPlusXag) {
        this.xgPlusXag = xgPlusXag;
    }

    public Long getNpxgPlusXag() {
        return npxgPlusXag;
    }

    public void setNpxgPlusXag(Long npxgPlusXag) {
        this.npxgPlusXag = npxgPlusXag;
    }

    public Long getPrgp() {
        return prgp;
    }

    public void setPrgp(Long prgp) {
        this.prgp = prgp;
    }

    public Long getPrgc() {
        return prgc;
    }

    public void setPrgc(Long prgc) {
        this.prgc = prgc;
    }

    public Long getPrgr() {
        return prgr;
    }

    public void setPrgr(Long prgr) {
        this.prgr = prgr;
    }

    public Long getGlsPer90() {
        return glsPer90;
    }

    public void setGlsPer90(Long glsPer90) {
        this.glsPer90 = glsPer90;
    }

    public Long getAstPer90() {
        return astPer90;
    }

    public void setAstPer90(Long astPer90) {
        this.astPer90 = astPer90;
    }

    public Long getgPlusAPer90() {
        return gPlusAPer90;
    }

    public void setgPlusAPer90(Long gPlusAPer90) {
        this.gPlusAPer90 = gPlusAPer90;
    }

    public Long getXgPer90() {
        return xgPer90;
    }

    public void setXgPer90(Long xgPer90) {
        this.xgPer90 = xgPer90;
    }

    public Long getXagPer90() {
        return xagPer90;
    }

    public void setXagPer90(Long xagPer90) {
        this.xagPer90 = xagPer90;
    }

    public Long getXgPlusXagPer90() {
        return xgPlusXagPer90;
    }

    public void setXgPlusXagPer90(Long xgPlusXagPer90) {
        this.xgPlusXagPer90 = xgPlusXagPer90;
    }

    public Long getXgDiff() {
        return xgDiff;
    }

    public void setXgDiff(Long xgDiff) {
        this.xgDiff = xgDiff;
    }

    public Long getXaDiff() {
        return xaDiff;
    }

    public void setXaDiff(Long xaDiff) {
        this.xaDiff = xaDiff;
    }

    public String getTrend() {
        return trend;
    }

    public void setTrend(String trend) {
        this.trend = trend;
    }

    public String getNewleague() {
        return newleague;
    }

    public void setNewleague(String newleague) {
        this.newleague = newleague;
    }

    public String getSourcefile() {
        return sourcefile;
    }

    public void setSourcefile(String sourcefile) {
        this.sourcefile = sourcefile;
    }

    public Role getRole() {
        return role;
    }

    public void setRole(Role role) {
        this.role = role;
    }

    public Long getCareerMp() {
        return careerMp;
    }

    public void setCareerMp(Long careerMp) {
        this.careerMp = careerMp;
    }

    public Long getCareerStarts() {
        return careerStarts;
    }

    public void setCareerStarts(Long careerStarts) {
        this.careerStarts = careerStarts;
    }

    public Long getCareerMin() {
        return careerMin;
    }

    public void setCareerMin(Long careerMin) {
        this.careerMin = careerMin;
    }

    public Long getCareer90s() {
        return career90s;
    }

    public void setCareer90s(Long career90s) {
        this.career90s = career90s;
    }

    public Long getCareerGls() {
        return careerGls;
    }

    public void setCareerGls(Long careerGls) {
        this.careerGls = careerGls;
    }

    public Long getCareerAst() {
        return careerAst;
    }

    public void setCareerAst(Long careerAst) {
        this.careerAst = careerAst;
    }

    public Long getCareerGPlusA() {
        return careerGPlusA;
    }

    public void setCareerGPlusA(Long careerGPlusA) {
        this.careerGPlusA = careerGPlusA;
    }

    public Long getCareerXg() {
        return careerXg;
    }

    public void setCareerXg(Long careerXg) {
        this.careerXg = careerXg;
    }

    public Long getCareerXag() {
        return careerXag;
    }

    public void setCareerXag(Long careerXag) {
        this.careerXag = careerXag;
    }

    public Long getCareerXgPlusXag() {
        return careerXgPlusXag;
    }

    public void setCareerXgPlusXag(Long careerXgPlusXag) {
        this.careerXgPlusXag = careerXgPlusXag;
    }

    public Long getCareerNpxg() {
        return careerNpxg;
    }

    public void setCareerNpxg(Long careerNpxg) {
        this.careerNpxg = careerNpxg;
    }

    public Long getCareerNpxgPlusXag() {
        return careerNpxgPlusXag;
    }

    public void setCareerNpxgPlusXag(Long careerNpxgPlusXag) {
        this.careerNpxgPlusXag = careerNpxgPlusXag;
    }

    public Long getCareerGlsPer90() {
        return careerGlsPer90;
    }

    public void setCareerGlsPer90(Long careerGlsPer90) {
        this.careerGlsPer90 = careerGlsPer90;
    }

    public Long getCareerAstPer90() {
        return careerAstPer90;
    }

    public void setCareerAstPer90(Long careerAstPer90) {
        this.careerAstPer90 = careerAstPer90;
    }

    public Long getCareerGPlusAPer90() {
        return careerGPlusAPer90;
    }

    public void setCareerGPlusAPer90(Long careerGPlusAPer90) {
        this.careerGPlusAPer90 = careerGPlusAPer90;
    }

    public Long getCareerXgPer90() {
        return careerXgPer90;
    }

    public void setCareerXgPer90(Long careerXgPer90) {
        this.careerXgPer90 = careerXgPer90;
    }

    public Long getCareerXagPer90() {
        return careerXagPer90;
    }

    public void setCareerXagPer90(Long careerXagPer90) {
        this.careerXagPer90 = careerXagPer90;
    }

    public Long getCareerXgPlusXagPer90() {
        return careerXgPlusXagPer90;
    }

    public void setCareerXgPlusXagPer90(Long careerXgPlusXagPer90) {
        this.careerXgPlusXagPer90 = careerXgPlusXagPer90;
    }

    public SquadraDTO getSquadra() {
        return squadra;
    }

    public void setSquadra(SquadraDTO squadra) {
        this.squadra = squadra;
    }

    public WatchListDTO getWatchList() {
        return watchList;
    }

    public void setWatchList(WatchListDTO watchList) {
        this.watchList = watchList;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof GiocatoreDTO)) {
            return false;
        }

        GiocatoreDTO giocatoreDTO = (GiocatoreDTO) o;
        if (this.id == null) {
            return false;
        }
        return Objects.equals(this.id, giocatoreDTO.id);
    }

    @Override
    public int hashCode() {
        return Objects.hash(this.id);
    }

    // prettier-ignore
    @Override
    public String toString() {
        return "GiocatoreDTO{" +
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
            ", squadra=" + getSquadra() +
            ", watchList=" + getWatchList() +
            "}";
    }
}
