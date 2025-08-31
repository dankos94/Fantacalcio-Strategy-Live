import React, { useEffect } from 'react';
import { Link, useParams } from 'react-router-dom';
import { Button, Col, Row } from 'reactstrap';
import {} from 'react-jhipster';
import { FontAwesomeIcon } from '@fortawesome/react-fontawesome';

import { useAppDispatch, useAppSelector } from 'app/config/store';

import { getEntity } from './giocatore.reducer';

export const GiocatoreDetail = () => {
  const dispatch = useAppDispatch();

  const { id } = useParams<'id'>();

  useEffect(() => {
    dispatch(getEntity(id));
  }, []);

  const giocatoreEntity = useAppSelector(state => state.giocatore.entity);
  return (
    <Row>
      <Col md="8">
        <h2 data-cy="giocatoreDetailsHeading">Giocatore</h2>
        <dl className="jh-entity-details">
          <dt>
            <span id="id">ID</span>
          </dt>
          <dd>{giocatoreEntity.id}</dd>
          <dt>
            <span id="player">Player</span>
          </dt>
          <dd>{giocatoreEntity.player}</dd>
          <dt>
            <span id="team">Team</span>
          </dt>
          <dd>{giocatoreEntity.team}</dd>
          <dt>
            <span id="season">Season</span>
          </dt>
          <dd>{giocatoreEntity.season}</dd>
          <dt>
            <span id="age">Age</span>
          </dt>
          <dd>{giocatoreEntity.age}</dd>
          <dt>
            <span id="squad">Squad</span>
          </dt>
          <dd>{giocatoreEntity.squad}</dd>
          <dt>
            <span id="comp">Comp</span>
          </dt>
          <dd>{giocatoreEntity.comp}</dd>
          <dt>
            <span id="country">Country</span>
          </dt>
          <dd>{giocatoreEntity.country}</dd>
          <dt>
            <span id="mp">Mp</span>
          </dt>
          <dd>{giocatoreEntity.mp}</dd>
          <dt>
            <span id="starts">Starts</span>
          </dt>
          <dd>{giocatoreEntity.starts}</dd>
          <dt>
            <span id="startsPct">Starts Pct</span>
          </dt>
          <dd>{giocatoreEntity.startsPct}</dd>
          <dt>
            <span id="minutes">Minutes</span>
          </dt>
          <dd>{giocatoreEntity.minutes}</dd>
          <dt>
            <span id="nins">Nins</span>
          </dt>
          <dd>{giocatoreEntity.nins}</dd>
          <dt>
            <span id="gls">Gls</span>
          </dt>
          <dd>{giocatoreEntity.gls}</dd>
          <dt>
            <span id="ast">Ast</span>
          </dt>
          <dd>{giocatoreEntity.ast}</dd>
          <dt>
            <span id="gPlusA">G Plus A</span>
          </dt>
          <dd>{giocatoreEntity.gPlusA}</dd>
          <dt>
            <span id="gPk">G Pk</span>
          </dt>
          <dd>{giocatoreEntity.gPk}</dd>
          <dt>
            <span id="pk">Pk</span>
          </dt>
          <dd>{giocatoreEntity.pk}</dd>
          <dt>
            <span id="pkatt">Pkatt</span>
          </dt>
          <dd>{giocatoreEntity.pkatt}</dd>
          <dt>
            <span id="crdy">Crdy</span>
          </dt>
          <dd>{giocatoreEntity.crdy}</dd>
          <dt>
            <span id="crdr">Crdr</span>
          </dt>
          <dd>{giocatoreEntity.crdr}</dd>
          <dt>
            <span id="xg">Xg</span>
          </dt>
          <dd>{giocatoreEntity.xg}</dd>
          <dt>
            <span id="xag">Xag</span>
          </dt>
          <dd>{giocatoreEntity.xag}</dd>
          <dt>
            <span id="npxg">Npxg</span>
          </dt>
          <dd>{giocatoreEntity.npxg}</dd>
          <dt>
            <span id="xgPlusXag">Xg Plus Xag</span>
          </dt>
          <dd>{giocatoreEntity.xgPlusXag}</dd>
          <dt>
            <span id="npxgPlusXag">Npxg Plus Xag</span>
          </dt>
          <dd>{giocatoreEntity.npxgPlusXag}</dd>
          <dt>
            <span id="prgp">Prgp</span>
          </dt>
          <dd>{giocatoreEntity.prgp}</dd>
          <dt>
            <span id="prgc">Prgc</span>
          </dt>
          <dd>{giocatoreEntity.prgc}</dd>
          <dt>
            <span id="prgr">Prgr</span>
          </dt>
          <dd>{giocatoreEntity.prgr}</dd>
          <dt>
            <span id="glsPer90">Gls Per 90</span>
          </dt>
          <dd>{giocatoreEntity.glsPer90}</dd>
          <dt>
            <span id="astPer90">Ast Per 90</span>
          </dt>
          <dd>{giocatoreEntity.astPer90}</dd>
          <dt>
            <span id="gPlusAPer90">G Plus A Per 90</span>
          </dt>
          <dd>{giocatoreEntity.gPlusAPer90}</dd>
          <dt>
            <span id="xgPer90">Xg Per 90</span>
          </dt>
          <dd>{giocatoreEntity.xgPer90}</dd>
          <dt>
            <span id="xagPer90">Xag Per 90</span>
          </dt>
          <dd>{giocatoreEntity.xagPer90}</dd>
          <dt>
            <span id="xgPlusXagPer90">Xg Plus Xag Per 90</span>
          </dt>
          <dd>{giocatoreEntity.xgPlusXagPer90}</dd>
          <dt>
            <span id="xgDiff">Xg Diff</span>
          </dt>
          <dd>{giocatoreEntity.xgDiff}</dd>
          <dt>
            <span id="xaDiff">Xa Diff</span>
          </dt>
          <dd>{giocatoreEntity.xaDiff}</dd>
          <dt>
            <span id="trend">Trend</span>
          </dt>
          <dd>{giocatoreEntity.trend}</dd>
          <dt>
            <span id="newleague">Newleague</span>
          </dt>
          <dd>{giocatoreEntity.newleague}</dd>
          <dt>
            <span id="sourcefile">Sourcefile</span>
          </dt>
          <dd>{giocatoreEntity.sourcefile}</dd>
          <dt>
            <span id="role">Role</span>
          </dt>
          <dd>{giocatoreEntity.role}</dd>
          <dt>
            <span id="careerMp">Career Mp</span>
          </dt>
          <dd>{giocatoreEntity.careerMp}</dd>
          <dt>
            <span id="careerStarts">Career Starts</span>
          </dt>
          <dd>{giocatoreEntity.careerStarts}</dd>
          <dt>
            <span id="careerMin">Career Min</span>
          </dt>
          <dd>{giocatoreEntity.careerMin}</dd>
          <dt>
            <span id="career90s">Career 90 S</span>
          </dt>
          <dd>{giocatoreEntity.career90s}</dd>
          <dt>
            <span id="careerGls">Career Gls</span>
          </dt>
          <dd>{giocatoreEntity.careerGls}</dd>
          <dt>
            <span id="careerAst">Career Ast</span>
          </dt>
          <dd>{giocatoreEntity.careerAst}</dd>
          <dt>
            <span id="careerGPlusA">Career G Plus A</span>
          </dt>
          <dd>{giocatoreEntity.careerGPlusA}</dd>
          <dt>
            <span id="careerXg">Career Xg</span>
          </dt>
          <dd>{giocatoreEntity.careerXg}</dd>
          <dt>
            <span id="careerXag">Career Xag</span>
          </dt>
          <dd>{giocatoreEntity.careerXag}</dd>
          <dt>
            <span id="careerXgPlusXag">Career Xg Plus Xag</span>
          </dt>
          <dd>{giocatoreEntity.careerXgPlusXag}</dd>
          <dt>
            <span id="careerNpxg">Career Npxg</span>
          </dt>
          <dd>{giocatoreEntity.careerNpxg}</dd>
          <dt>
            <span id="careerNpxgPlusXag">Career Npxg Plus Xag</span>
          </dt>
          <dd>{giocatoreEntity.careerNpxgPlusXag}</dd>
          <dt>
            <span id="careerGlsPer90">Career Gls Per 90</span>
          </dt>
          <dd>{giocatoreEntity.careerGlsPer90}</dd>
          <dt>
            <span id="careerAstPer90">Career Ast Per 90</span>
          </dt>
          <dd>{giocatoreEntity.careerAstPer90}</dd>
          <dt>
            <span id="careerGPlusAPer90">Career G Plus A Per 90</span>
          </dt>
          <dd>{giocatoreEntity.careerGPlusAPer90}</dd>
          <dt>
            <span id="careerXgPer90">Career Xg Per 90</span>
          </dt>
          <dd>{giocatoreEntity.careerXgPer90}</dd>
          <dt>
            <span id="careerXagPer90">Career Xag Per 90</span>
          </dt>
          <dd>{giocatoreEntity.careerXagPer90}</dd>
          <dt>
            <span id="careerXgPlusXagPer90">Career Xg Plus Xag Per 90</span>
          </dt>
          <dd>{giocatoreEntity.careerXgPlusXagPer90}</dd>
          <dt>Squadra</dt>
          <dd>{giocatoreEntity.squadra ? giocatoreEntity.squadra.id : ''}</dd>
          <dt>Watch List</dt>
          <dd>{giocatoreEntity.watchList ? giocatoreEntity.watchList.id : ''}</dd>
        </dl>
        <Button tag={Link} to="/giocatore" replace color="info" data-cy="entityDetailsBackButton">
          <FontAwesomeIcon icon="arrow-left" /> <span className="d-none d-md-inline">Indietro</span>
        </Button>
        &nbsp;
        <Button tag={Link} to={`/giocatore/${giocatoreEntity.id}/edit`} replace color="primary">
          <FontAwesomeIcon icon="pencil-alt" /> <span className="d-none d-md-inline">Modifica</span>
        </Button>
      </Col>
    </Row>
  );
};

export default GiocatoreDetail;
