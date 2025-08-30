import React, { useEffect } from 'react';
import { Link, useParams } from 'react-router-dom';
import { Button, Col, Row } from 'reactstrap';
import {} from 'react-jhipster';
import { FontAwesomeIcon } from '@fortawesome/react-fontawesome';

import { useAppDispatch, useAppSelector } from 'app/config/store';

import { getEntity } from './roster.reducer';

export const RosterDetail = () => {
  const dispatch = useAppDispatch();

  const { id } = useParams<'id'>();

  useEffect(() => {
    dispatch(getEntity(id));
  }, []);

  const rosterEntity = useAppSelector(state => state.roster.entity);
  return (
    <Row>
      <Col md="8">
        <h2 data-cy="rosterDetailsHeading">Roster</h2>
        <dl className="jh-entity-details">
          <dt>
            <span id="id">ID</span>
          </dt>
          <dd>{rosterEntity.id}</dd>
          <dt>
            <span id="full">Full</span>
          </dt>
          <dd>{rosterEntity.full ? 'true' : 'false'}</dd>
          <dt>
            <span id="port">Port</span>
          </dt>
          <dd>{rosterEntity.port}</dd>
          <dt>
            <span id="dif">Dif</span>
          </dt>
          <dd>{rosterEntity.dif}</dd>
          <dt>
            <span id="cc">Cc</span>
          </dt>
          <dd>{rosterEntity.cc}</dd>
          <dt>
            <span id="att">Att</span>
          </dt>
          <dd>{rosterEntity.att}</dd>
          <dt>Squadra</dt>
          <dd>{rosterEntity.squadra ? rosterEntity.squadra.id : ''}</dd>
        </dl>
        <Button tag={Link} to="/roster" replace color="info" data-cy="entityDetailsBackButton">
          <FontAwesomeIcon icon="arrow-left" /> <span className="d-none d-md-inline">Indietro</span>
        </Button>
        &nbsp;
        <Button tag={Link} to={`/roster/${rosterEntity.id}/edit`} replace color="primary">
          <FontAwesomeIcon icon="pencil-alt" /> <span className="d-none d-md-inline">Modifica</span>
        </Button>
      </Col>
    </Row>
  );
};

export default RosterDetail;
