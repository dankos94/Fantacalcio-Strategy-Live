import React, { useEffect } from 'react';
import { Link, useNavigate, useParams } from 'react-router-dom';
import { Button, Col, Row } from 'reactstrap';
import { ValidatedField, ValidatedForm, isNumber } from 'react-jhipster';
import { FontAwesomeIcon } from '@fortawesome/react-fontawesome';

import { useAppDispatch, useAppSelector } from 'app/config/store';

import { getEntities as getSquadras } from 'app/entities/squadra/squadra.reducer';
import { createEntity, getEntity, reset, updateEntity } from './roster.reducer';

export const RosterUpdate = () => {
  const dispatch = useAppDispatch();

  const navigate = useNavigate();

  const { id } = useParams<'id'>();
  const isNew = id === undefined;

  const squadras = useAppSelector(state => state.squadra.entities);
  const rosterEntity = useAppSelector(state => state.roster.entity);
  const loading = useAppSelector(state => state.roster.loading);
  const updating = useAppSelector(state => state.roster.updating);
  const updateSuccess = useAppSelector(state => state.roster.updateSuccess);

  const handleClose = () => {
    navigate('/roster');
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
    if (values.port !== undefined && typeof values.port !== 'number') {
      values.port = Number(values.port);
    }
    if (values.dif !== undefined && typeof values.dif !== 'number') {
      values.dif = Number(values.dif);
    }
    if (values.cc !== undefined && typeof values.cc !== 'number') {
      values.cc = Number(values.cc);
    }
    if (values.att !== undefined && typeof values.att !== 'number') {
      values.att = Number(values.att);
    }

    const entity = {
      ...rosterEntity,
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
          ...rosterEntity,
          squadra: rosterEntity?.squadra?.id,
        };

  return (
    <div>
      <Row className="justify-content-center">
        <Col md="8">
          <h2 id="astaAppApp.roster.home.createOrEditLabel" data-cy="RosterCreateUpdateHeading">
            Genera o modifica un Roster
          </h2>
        </Col>
      </Row>
      <Row className="justify-content-center">
        <Col md="8">
          {loading ? (
            <p>Loading...</p>
          ) : (
            <ValidatedForm defaultValues={defaultValues()} onSubmit={saveEntity}>
              {!isNew ? <ValidatedField name="id" required readOnly id="roster-id" label="ID" validate={{ required: true }} /> : null}
              <ValidatedField label="Full" id="roster-full" name="full" data-cy="full" check type="checkbox" />
              <ValidatedField
                label="Port"
                id="roster-port"
                name="port"
                data-cy="port"
                type="text"
                validate={{
                  required: { value: true, message: 'Questo campo è obbligatorio.' },
                  validate: v => isNumber(v) || 'Questo campo dovrebbe essere un numero.',
                }}
              />
              <ValidatedField
                label="Dif"
                id="roster-dif"
                name="dif"
                data-cy="dif"
                type="text"
                validate={{
                  required: { value: true, message: 'Questo campo è obbligatorio.' },
                  validate: v => isNumber(v) || 'Questo campo dovrebbe essere un numero.',
                }}
              />
              <ValidatedField
                label="Cc"
                id="roster-cc"
                name="cc"
                data-cy="cc"
                type="text"
                validate={{
                  required: { value: true, message: 'Questo campo è obbligatorio.' },
                  validate: v => isNumber(v) || 'Questo campo dovrebbe essere un numero.',
                }}
              />
              <ValidatedField
                label="Att"
                id="roster-att"
                name="att"
                data-cy="att"
                type="text"
                validate={{
                  required: { value: true, message: 'Questo campo è obbligatorio.' },
                  validate: v => isNumber(v) || 'Questo campo dovrebbe essere un numero.',
                }}
              />
              <ValidatedField id="roster-squadra" name="squadra" data-cy="squadra" label="Squadra" type="select">
                <option value="" key="0" />
                {squadras
                  ? squadras.map(otherEntity => (
                      <option value={otherEntity.id} key={otherEntity.id}>
                        {otherEntity.id}
                      </option>
                    ))
                  : null}
              </ValidatedField>
              <Button tag={Link} id="cancel-save" data-cy="entityCreateCancelButton" to="/roster" replace color="info">
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

export default RosterUpdate;
