import React, { useEffect } from 'react';
import { Link, useParams } from 'react-router-dom';
import { Button, Col, Row } from 'reactstrap';
import {} from 'react-jhipster';
import { FontAwesomeIcon } from '@fortawesome/react-fontawesome';

import { useAppDispatch, useAppSelector } from 'app/config/store';

import { getEntity } from './coach.reducer';

export const CoachDetail = () => {
  const dispatch = useAppDispatch();

  const { id } = useParams<'id'>();

  useEffect(() => {
    dispatch(getEntity(id));
  }, []);

  const coachEntity = useAppSelector(state => state.coach.entity);
  return (
    <Row>
      <Col md="8">
        <h2 data-cy="coachDetailsHeading">Coach</h2>
        <dl className="jh-entity-details">
          <dt>
            <span id="id">ID</span>
          </dt>
          <dd>{coachEntity.id}</dd>
          <dt>
            <span id="nome">Nome</span>
          </dt>
          <dd>{coachEntity.nome}</dd>
          <dt>
            <span id="cognome">Cognome</span>
          </dt>
          <dd>{coachEntity.cognome}</dd>
          <dt>
            <span id="itsMe">Its Me</span>
          </dt>
          <dd>{coachEntity.itsMe ? 'true' : 'false'}</dd>
          <dt>Squadra</dt>
          <dd>{coachEntity.squadra ? coachEntity.squadra.id : ''}</dd>
        </dl>
        <Button tag={Link} to="/coach" replace color="info" data-cy="entityDetailsBackButton">
          <FontAwesomeIcon icon="arrow-left" /> <span className="d-none d-md-inline">Indietro</span>
        </Button>
        &nbsp;
        <Button tag={Link} to={`/coach/${coachEntity.id}/edit`} replace color="primary">
          <FontAwesomeIcon icon="pencil-alt" /> <span className="d-none d-md-inline">Modifica</span>
        </Button>
      </Col>
    </Row>
  );
};

export default CoachDetail;
