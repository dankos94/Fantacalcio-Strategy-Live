import React, { useEffect } from 'react';
import { Link, useParams } from 'react-router-dom';
import { Button, Col, Row } from 'reactstrap';
import {} from 'react-jhipster';
import { FontAwesomeIcon } from '@fortawesome/react-fontawesome';

import { useAppDispatch, useAppSelector } from 'app/config/store';

import { getEntity } from './lega.reducer';

export const LegaDetail = () => {
  const dispatch = useAppDispatch();

  const { id } = useParams<'id'>();

  useEffect(() => {
    dispatch(getEntity(id));
  }, []);

  const legaEntity = useAppSelector(state => state.lega.entity);
  return (
    <Row>
      <Col md="8">
        <h2 data-cy="legaDetailsHeading">Lega</h2>
        <dl className="jh-entity-details">
          <dt>
            <span id="id">ID</span>
          </dt>
          <dd>{legaEntity.id}</dd>
          <dt>
            <span id="nome">Nome</span>
          </dt>
          <dd>{legaEntity.nome}</dd>
          <dt>
            <span id="budget">Budget</span>
          </dt>
          <dd>{legaEntity.budget}</dd>
          <dt>Stagione</dt>
          <dd>{legaEntity.stagione ? legaEntity.stagione.id : ''}</dd>
        </dl>
        <Button tag={Link} to="/lega" replace color="info" data-cy="entityDetailsBackButton">
          <FontAwesomeIcon icon="arrow-left" /> <span className="d-none d-md-inline">Indietro</span>
        </Button>
        &nbsp;
        <Button tag={Link} to={`/lega/${legaEntity.id}/edit`} replace color="primary">
          <FontAwesomeIcon icon="pencil-alt" /> <span className="d-none d-md-inline">Modifica</span>
        </Button>
      </Col>
    </Row>
  );
};

export default LegaDetail;
