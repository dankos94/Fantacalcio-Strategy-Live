import React, { useEffect } from 'react';
import { Link, useNavigate, useParams } from 'react-router-dom';
import { Button, Col, Row } from 'reactstrap';
import { ValidatedField, ValidatedForm } from 'react-jhipster';
import { FontAwesomeIcon } from '@fortawesome/react-fontawesome';

import { useAppDispatch, useAppSelector } from 'app/config/store';

import { getEntities as getSquadras } from 'app/entities/squadra/squadra.reducer';
import { createEntity, getEntity, reset, updateEntity } from './coach.reducer';

export const CoachUpdate = () => {
  const dispatch = useAppDispatch();

  const navigate = useNavigate();

  const { id } = useParams<'id'>();
  const isNew = id === undefined;

  const squadras = useAppSelector(state => state.squadra.entities);
  const coachEntity = useAppSelector(state => state.coach.entity);
  const loading = useAppSelector(state => state.coach.loading);
  const updating = useAppSelector(state => state.coach.updating);
  const updateSuccess = useAppSelector(state => state.coach.updateSuccess);

  const handleClose = () => {
    navigate('/coach');
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

    const entity = {
      ...coachEntity,
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
      ? {}
      : {
          ...coachEntity,
          squadra: coachEntity?.squadra?.id,
        };

  return (
    <div>
      <Row className="justify-content-center">
        <Col md="8">
          <h2 id="astaAppApp.coach.home.createOrEditLabel" data-cy="CoachCreateUpdateHeading">
            Genera o modifica un Coach
          </h2>
        </Col>
      </Row>
      <Row className="justify-content-center">
        <Col md="8">
          {loading ? (
            <p>Loading...</p>
          ) : (
            <ValidatedForm defaultValues={defaultValues()} onSubmit={saveEntity}>
              {!isNew ? <ValidatedField name="id" required readOnly id="coach-id" label="ID" validate={{ required: true }} /> : null}
              <ValidatedField
                label="Nome"
                id="coach-nome"
                name="nome"
                data-cy="nome"
                type="text"
                validate={{
                  required: { value: true, message: 'Questo campo è obbligatorio.' },
                }}
              />
              <ValidatedField label="Cognome" id="coach-cognome" name="cognome" data-cy="cognome" type="text" />
              <ValidatedField label="Its Me" id="coach-itsMe" name="itsMe" data-cy="itsMe" check type="checkbox" />
              <ValidatedField id="coach-squadra" name="squadra" data-cy="squadra" label="Squadra" type="select">
                <option value="" key="0" />
                {squadras
                  ? squadras.map(otherEntity => (
                      <option value={otherEntity.id} key={otherEntity.id}>
                        {otherEntity.id}
                      </option>
                    ))
                  : null}
              </ValidatedField>
              <Button tag={Link} id="cancel-save" data-cy="entityCreateCancelButton" to="/coach" replace color="info">
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

export default CoachUpdate;
