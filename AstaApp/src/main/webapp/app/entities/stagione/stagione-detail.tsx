import React, { useEffect } from 'react';
import { Link, useParams } from 'react-router-dom';
import { Button, Col, Row } from 'reactstrap';
import {} from 'react-jhipster';
import { FontAwesomeIcon } from '@fortawesome/react-fontawesome';

import { useAppDispatch, useAppSelector } from 'app/config/store';

import { getEntity } from './stagione.reducer';

export const StagioneDetail = () => {
  const dispatch = useAppDispatch();

  const { id } = useParams<'id'>();

  useEffect(() => {
    dispatch(getEntity(id));
  }, []);

  const stagioneEntity = useAppSelector(state => state.stagione.entity);
  return (
    <Row>
      <Col md="8">
        <h2 data-cy="stagioneDetailsHeading">Stagione</h2>
        <dl className="jh-entity-details">
          <dt>
            <span id="id">ID</span>
          </dt>
          <dd>{stagioneEntity.id}</dd>
          <dt>
            <span id="nome">Nome</span>
          </dt>
          <dd>{stagioneEntity.nome}</dd>
        </dl>
        <Button tag={Link} to="/stagione" replace color="info" data-cy="entityDetailsBackButton">
          <FontAwesomeIcon icon="arrow-left" /> <span className="d-none d-md-inline">Indietro</span>
        </Button>
        &nbsp;
        <Button tag={Link} to={`/stagione/${stagioneEntity.id}/edit`} replace color="primary">
          <FontAwesomeIcon icon="pencil-alt" /> <span className="d-none d-md-inline">Modifica</span>
        </Button>
      </Col>
    </Row>
  );
};

export default StagioneDetail;
