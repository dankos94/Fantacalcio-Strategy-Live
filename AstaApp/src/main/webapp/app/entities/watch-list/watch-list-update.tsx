import React, { useEffect } from 'react';
import { Link, useNavigate, useParams } from 'react-router-dom';
import { Button, Col, Row } from 'reactstrap';
import { ValidatedField, ValidatedForm } from 'react-jhipster';
import { FontAwesomeIcon } from '@fortawesome/react-fontawesome';

import { convertDateTimeFromServer, convertDateTimeToServer, displayDefaultDateTime } from 'app/shared/util/date-utils';
import { useAppDispatch, useAppSelector } from 'app/config/store';

import { getEntities as getSquadras } from 'app/entities/squadra/squadra.reducer';
import { createEntity, getEntity, reset, updateEntity } from './watch-list.reducer';

export const WatchListUpdate = () => {
  const dispatch = useAppDispatch();

  const navigate = useNavigate();

  const { id } = useParams<'id'>();
  const isNew = id === undefined;

  const squadras = useAppSelector(state => state.squadra.entities);
  const watchListEntity = useAppSelector(state => state.watchList.entity);
  const loading = useAppSelector(state => state.watchList.loading);
  const updating = useAppSelector(state => state.watchList.updating);
  const updateSuccess = useAppSelector(state => state.watchList.updateSuccess);

  const handleClose = () => {
    navigate('/watch-list');
  };

  useEffect(() => {
    if (isNew) {
      dispatch(reset());
    } else {
      dispatch(getEntity(id));
    }

    dispatch(getSquadras({}));
  }, []);

  useEffect(() => {
    if (updateSuccess) {
      handleClose();
    }
  }, [updateSuccess]);

  const saveEntity = values => {
    if (values.id !== undefined && typeof values.id !== 'number') {
      values.id = Number(values.id);
    }
    values.date = convertDateTimeToServer(values.date);

    const entity = {
      ...watchListEntity,
      ...values,
      squadra: squadras.find(it => it.id.toString() === values.squadra?.toString()),
    };

    if (isNew) {
      dispatch(createEntity(entity));
    } else {
      dispatch(updateEntity(entity));
    }
  };

  const defaultValues = () =>
    isNew
      ? {
          date: displayDefaultDateTime(),
        }
      : {
          ...watchListEntity,
          date: convertDateTimeFromServer(watchListEntity.date),
          squadra: watchListEntity?.squadra?.id,
        };

  return (
    <div>
      <Row className="justify-content-center">
        <Col md="8">
          <h2 id="astaAppApp.watchList.home.createOrEditLabel" data-cy="WatchListCreateUpdateHeading">
            Genera o modifica un Watch List
          </h2>
        </Col>
      </Row>
      <Row className="justify-content-center">
        <Col md="8">
          {loading ? (
            <p>Loading...</p>
          ) : (
            <ValidatedForm defaultValues={defaultValues()} onSubmit={saveEntity}>
              {!isNew ? <ValidatedField name="id" required readOnly id="watch-list-id" label="ID" validate={{ required: true }} /> : null}
              <ValidatedField
                label="Date"
                id="watch-list-date"
                name="date"
                data-cy="date"
                type="datetime-local"
                placeholder="YYYY-MM-DD HH:mm"
              />
              <ValidatedField label="Version" id="watch-list-version" name="version" data-cy="version" type="text" />
              <ValidatedField id="watch-list-squadra" name="squadra" data-cy="squadra" label="Squadra" type="select">
                <option value="" key="0" />
                {squadras
                  ? squadras.map(otherEntity => (
                      <option value={otherEntity.id} key={otherEntity.id}>
                        {otherEntity.id}
                      </option>
                    ))
                  : null}
              </ValidatedField>
              <Button tag={Link} id="cancel-save" data-cy="entityCreateCancelButton" to="/watch-list" replace color="info">
                <FontAwesomeIcon icon="arrow-left" />
                &nbsp;
                <span className="d-none d-md-inline">Indietro</span>
              </Button>
              &nbsp;
              <Button color="primary" id="save-entity" data-cy="entityCreateSaveButton" type="submit" disabled={updating}>
                <FontAwesomeIcon icon="save" />
                &nbsp; Salva
              </Button>
            </ValidatedForm>
          )}
        </Col>
      </Row>
    </div>
  );
};

export default WatchListUpdate;
