import React, { useEffect } from 'react';
import { Link, useParams } from 'react-router-dom';
import { Button, Col, Row } from 'reactstrap';
import {} from 'react-jhipster';
import { FontAwesomeIcon } from '@fortawesome/react-fontawesome';

import { useAppDispatch, useAppSelector } from 'app/config/store';

import { getEntity } from './squadra.reducer';

export const SquadraDetail = () => {
  const dispatch = useAppDispatch();

  const { id } = useParams<'id'>();

  useEffect(() => {
    dispatch(getEntity(id));
  }, []);

  const squadraEntity = useAppSelector(state => state.squadra.entity);
  return (
    <Row>
      <Col md="8">
        <h2 data-cy="squadraDetailsHeading">Squadra</h2>
        <dl className="jh-entity-details">
          <dt>
            <span id="id">ID</span>
          </dt>
          <dd>{squadraEntity.id}</dd>
          <dt>
            <span id="nome">Nome</span>
          </dt>
          <dd>{squadraEntity.nome}</dd>
          <dt>Lega</dt>
          <dd>{squadraEntity.lega ? squadraEntity.lega.id : ''}</dd>
        </dl>
        <Button tag={Link} to="/squadra" replace color="info" data-cy="entityDetailsBackButton">
          <FontAwesomeIcon icon="arrow-left" /> <span className="d-none d-md-inline">Indietro</span>
        </Button>
        &nbsp;
        <Button tag={Link} to={`/squadra/${squadraEntity.id}/edit`} replace color="primary">
          <FontAwesomeIcon icon="pencil-alt" /> <span className="d-none d-md-inline">Modifica</span>
        </Button>
      </Col>
    </Row>
  );
};

export default SquadraDetail;
