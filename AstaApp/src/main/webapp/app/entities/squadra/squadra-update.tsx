import React, { useEffect } from 'react';
import { Link, useNavigate, useParams } from 'react-router-dom';
import { Button, Col, Row } from 'reactstrap';
import { ValidatedField, ValidatedForm } from 'react-jhipster';
import { FontAwesomeIcon } from '@fortawesome/react-fontawesome';

import { useAppDispatch, useAppSelector } from 'app/config/store';

import { getEntities as getLegas } from 'app/entities/lega/lega.reducer';
import { createEntity, getEntity, reset, updateEntity } from './squadra.reducer';

export const SquadraUpdate = () => {
  const dispatch = useAppDispatch();

  const navigate = useNavigate();

  const { id } = useParams<'id'>();
  const isNew = id === undefined;

  const legas = useAppSelector(state => state.lega.entities);
  const squadraEntity = useAppSelector(state => state.squadra.entity);
  const loading = useAppSelector(state => state.squadra.loading);
  const updating = useAppSelector(state => state.squadra.updating);
  const updateSuccess = useAppSelector(state => state.squadra.updateSuccess);

  const handleClose = () => {
    navigate('/squadra');
  };

  useEffect(() => {
    if (isNew) {
      dispatch(reset());
    } else {
      dispatch(getEntity(id));
    }

    dispatch(getLegas({}));
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
      ...squadraEntity,
      ...values,
      lega: legas.find(it => it.id.toString() === values.lega?.toString()),
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
          ...squadraEntity,
          lega: squadraEntity?.lega?.id,
        };

  return (
    <div>
      <Row className="justify-content-center">
        <Col md="8">
          <h2 id="astaAppApp.squadra.home.createOrEditLabel" data-cy="SquadraCreateUpdateHeading">
            Genera o modifica un Squadra
          </h2>
        </Col>
      </Row>
      <Row className="justify-content-center">
        <Col md="8">
          {loading ? (
            <p>Loading...</p>
          ) : (
            <ValidatedForm defaultValues={defaultValues()} onSubmit={saveEntity}>
              {!isNew ? <ValidatedField name="id" required readOnly id="squadra-id" label="ID" validate={{ required: true }} /> : null}
              <ValidatedField
                label="Nome"
                id="squadra-nome"
                name="nome"
                data-cy="nome"
                type="text"
                validate={{
                  required: { value: true, message: 'Questo campo è obbligatorio.' },
                }}
              />
              <ValidatedField id="squadra-lega" name="lega" data-cy="lega" label="Lega" type="select">
                <option value="" key="0" />
                {legas
                  ? legas.map(otherEntity => (
                      <option value={otherEntity.id} key={otherEntity.id}>
                        {otherEntity.id}
                      </option>
                    ))
                  : null}
              </ValidatedField>
              <Button tag={Link} id="cancel-save" data-cy="entityCreateCancelButton" to="/squadra" replace color="info">
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

export default SquadraUpdate;
