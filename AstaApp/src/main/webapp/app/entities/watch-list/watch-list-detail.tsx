import React, { useEffect } from 'react';
import { Link, useParams } from 'react-router-dom';
import { Button, Col, Row } from 'reactstrap';
import { TextFormat } from 'react-jhipster';
import { FontAwesomeIcon } from '@fortawesome/react-fontawesome';

import { APP_DATE_FORMAT } from 'app/config/constants';
import { useAppDispatch, useAppSelector } from 'app/config/store';

import { getEntity } from './watch-list.reducer';

export const WatchListDetail = () => {
  const dispatch = useAppDispatch();

  const { id } = useParams<'id'>();

  useEffect(() => {
    dispatch(getEntity(id));
  }, []);

  const watchListEntity = useAppSelector(state => state.watchList.entity);
  return (
    <Row>
      <Col md="8">
        <h2 data-cy="watchListDetailsHeading">Watch List</h2>
        <dl className="jh-entity-details">
          <dt>
            <span id="id">ID</span>
          </dt>
          <dd>{watchListEntity.id}</dd>
          <dt>
            <span id="date">Date</span>
          </dt>
          <dd>{watchListEntity.date ? <TextFormat value={watchListEntity.date} type="date" format={APP_DATE_FORMAT} /> : null}</dd>
          <dt>
            <span id="version">Version</span>
          </dt>
          <dd>{watchListEntity.version}</dd>
          <dt>Squadra</dt>
          <dd>{watchListEntity.squadra ? watchListEntity.squadra.id : ''}</dd>
        </dl>
        <Button tag={Link} to="/watch-list" replace color="info" data-cy="entityDetailsBackButton">
          <FontAwesomeIcon icon="arrow-left" /> <span className="d-none d-md-inline">Indietro</span>
        </Button>
        &nbsp;
        <Button tag={Link} to={`/watch-list/${watchListEntity.id}/edit`} replace color="primary">
          <FontAwesomeIcon icon="pencil-alt" /> <span className="d-none d-md-inline">Modifica</span>
        </Button>
      </Col>
    </Row>
  );
};

export default WatchListDetail;
