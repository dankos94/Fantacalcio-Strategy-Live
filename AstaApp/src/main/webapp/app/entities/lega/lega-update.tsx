import React, { useEffect } from 'react';
import { Link, useNavigate, useParams } from 'react-router-dom';
import { Button, Col, Row } from 'reactstrap';
import { ValidatedField, ValidatedForm, isNumber } from 'react-jhipster';
import { FontAwesomeIcon } from '@fortawesome/react-fontawesome';

import { useAppDispatch, useAppSelector } from 'app/config/store';

import { getEntities as getStagiones } from 'app/entities/stagione/stagione.reducer';
import { createEntity, getEntity, reset, updateEntity } from './lega.reducer';

export const LegaUpdate = () => {
  const dispatch = useAppDispatch();

  const navigate = useNavigate();

  const { id } = useParams<'id'>();
  const isNew = id === undefined;

  const stagiones = useAppSelector(state => state.stagione.entities);
  const legaEntity = useAppSelector(state => state.lega.entity);
  const loading = useAppSelector(state => state.lega.loading);
  const updating = useAppSelector(state => state.lega.updating);
  const updateSuccess = useAppSelector(state => state.lega.updateSuccess);

  const handleClose = () => {
    navigate('/lega');
  };

  useEffect(() => {
    if (isNew) {
      dispatch(reset());
    } else {
      dispatch(getEntity(id));
    }

    dispatch(getStagiones({}));
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
    if (values.budget !== undefined && typeof values.budget !== 'number') {
      values.budget = Number(values.budget);
    }

    const entity = {
      ...legaEntity,
      ...values,
      stagione: stagiones.find(it => it.id.toString() === values.stagione?.toString()),
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
          ...legaEntity,
          stagione: legaEntity?.stagione?.id,
        };

  return (
    <div>
      <Row className="justify-content-center">
        <Col md="8">
          <h2 id="astaAppApp.lega.home.createOrEditLabel" data-cy="LegaCreateUpdateHeading">
            Genera o modifica un Lega
          </h2>
        </Col>
      </Row>
      <Row className="justify-content-center">
        <Col md="8">
          {loading ? (
            <p>Loading...</p>
          ) : (
            <ValidatedForm defaultValues={defaultValues()} onSubmit={saveEntity}>
              {!isNew ? <ValidatedField name="id" required readOnly id="lega-id" label="ID" validate={{ required: true }} /> : null}
              <ValidatedField
                label="Nome"
                id="lega-nome"
                name="nome"
                data-cy="nome"
                type="text"
                validate={{
                  required: { value: true, message: 'Questo campo è obbligatorio.' },
                }}
              />
              <ValidatedField
                label="Budget"
                id="lega-budget"
                name="budget"
                data-cy="budget"
                type="text"
                validate={{
                  required: { value: true, message: 'Questo campo è obbligatorio.' },
                  validate: v => isNumber(v) || 'Questo campo dovrebbe essere un numero.',
                }}
              />
              <ValidatedField id="lega-stagione" name="stagione" data-cy="stagione" label="Stagione" type="select">
                <option value="" key="0" />
                {stagiones
                  ? stagiones.map(otherEntity => (
                      <option value={otherEntity.id} key={otherEntity.id}>
                        {otherEntity.id}
                      </option>
                    ))
                  : null}
              </ValidatedField>
              <Button tag={Link} id="cancel-save" data-cy="entityCreateCancelButton" to="/lega" replace color="info">
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

export default LegaUpdate;
