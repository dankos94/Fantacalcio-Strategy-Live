import React, { useEffect } from 'react';
import { Link, useNavigate, useParams } from 'react-router-dom';
import { Button, Col, Row } from 'reactstrap';
import { ValidatedField, ValidatedForm } from 'react-jhipster';
import { FontAwesomeIcon } from '@fortawesome/react-fontawesome';

import { useAppDispatch, useAppSelector } from 'app/config/store';

import { getEntities as getSquadras } from 'app/entities/squadra/squadra.reducer';
import { getEntities as getWatchLists } from 'app/entities/watch-list/watch-list.reducer';
import { Role } from 'app/shared/model/enumerations/role.model';
import { createEntity, getEntity, reset, updateEntity } from './giocatore.reducer';

export const GiocatoreUpdate = () => {
  const dispatch = useAppDispatch();

  const navigate = useNavigate();

  const { id } = useParams<'id'>();
  const isNew = id === undefined;

  const squadras = useAppSelector(state => state.squadra.entities);
  const watchLists = useAppSelector(state => state.watchList.entities);
  const giocatoreEntity = useAppSelector(state => state.giocatore.entity);
  const loading = useAppSelector(state => state.giocatore.loading);
  const updating = useAppSelector(state => state.giocatore.updating);
  const updateSuccess = useAppSelector(state => state.giocatore.updateSuccess);
  const roleValues = Object.keys(Role);

  const handleClose = () => {
    navigate('/giocatore');
  };

  useEffect(() => {
    if (isNew) {
      dispatch(reset());
    } else {
      dispatch(getEntity(id));
    }

    dispatch(getSquadras({}));
    dispatch(getWatchLists({}));
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
    if (values.mp !== undefined && typeof values.mp !== 'number') {
      values.mp = Number(values.mp);
    }
    if (values.starts !== undefined && typeof values.starts !== 'number') {
      values.starts = Number(values.starts);
    }
    if (values.startsPct !== undefined && typeof values.startsPct !== 'number') {
      values.startsPct = Number(values.startsPct);
    }
    if (values.minutes !== undefined && typeof values.minutes !== 'number') {
      values.minutes = Number(values.minutes);
    }
    if (values.nins !== undefined && typeof values.nins !== 'number') {
      values.nins = Number(values.nins);
    }
    if (values.gls !== undefined && typeof values.gls !== 'number') {
      values.gls = Number(values.gls);
    }
    if (values.ast !== undefined && typeof values.ast !== 'number') {
      values.ast = Number(values.ast);
    }
    if (values.gPlusA !== undefined && typeof values.gPlusA !== 'number') {
      values.gPlusA = Number(values.gPlusA);
    }
    if (values.gPk !== undefined && typeof values.gPk !== 'number') {
      values.gPk = Number(values.gPk);
    }
    if (values.pk !== undefined && typeof values.pk !== 'number') {
      values.pk = Number(values.pk);
    }
    if (values.pkatt !== undefined && typeof values.pkatt !== 'number') {
      values.pkatt = Number(values.pkatt);
    }
    if (values.crdy !== undefined && typeof values.crdy !== 'number') {
      values.crdy = Number(values.crdy);
    }
    if (values.crdr !== undefined && typeof values.crdr !== 'number') {
      values.crdr = Number(values.crdr);
    }
    if (values.xg !== undefined && typeof values.xg !== 'number') {
      values.xg = Number(values.xg);
    }
    if (values.xag !== undefined && typeof values.xag !== 'number') {
      values.xag = Number(values.xag);
    }
    if (values.npxg !== undefined && typeof values.npxg !== 'number') {
      values.npxg = Number(values.npxg);
    }
    if (values.xgPlusXag !== undefined && typeof values.xgPlusXag !== 'number') {
      values.xgPlusXag = Number(values.xgPlusXag);
    }
    if (values.npxgPlusXag !== undefined && typeof values.npxgPlusXag !== 'number') {
      values.npxgPlusXag = Number(values.npxgPlusXag);
    }
    if (values.prgp !== undefined && typeof values.prgp !== 'number') {
      values.prgp = Number(values.prgp);
    }
    if (values.prgc !== undefined && typeof values.prgc !== 'number') {
      values.prgc = Number(values.prgc);
    }
    if (values.prgr !== undefined && typeof values.prgr !== 'number') {
      values.prgr = Number(values.prgr);
    }
    if (values.glsPer90 !== undefined && typeof values.glsPer90 !== 'number') {
      values.glsPer90 = Number(values.glsPer90);
    }
    if (values.astPer90 !== undefined && typeof values.astPer90 !== 'number') {
      values.astPer90 = Number(values.astPer90);
    }
    if (values.gPlusAPer90 !== undefined && typeof values.gPlusAPer90 !== 'number') {
      values.gPlusAPer90 = Number(values.gPlusAPer90);
    }
    if (values.xgPer90 !== undefined && typeof values.xgPer90 !== 'number') {
      values.xgPer90 = Number(values.xgPer90);
    }
    if (values.xagPer90 !== undefined && typeof values.xagPer90 !== 'number') {
      values.xagPer90 = Number(values.xagPer90);
    }
    if (values.xgPlusXagPer90 !== undefined && typeof values.xgPlusXagPer90 !== 'number') {
      values.xgPlusXagPer90 = Number(values.xgPlusXagPer90);
    }
    if (values.xgDiff !== undefined && typeof values.xgDiff !== 'number') {
      values.xgDiff = Number(values.xgDiff);
    }
    if (values.xaDiff !== undefined && typeof values.xaDiff !== 'number') {
      values.xaDiff = Number(values.xaDiff);
    }
    if (values.careerMp !== undefined && typeof values.careerMp !== 'number') {
      values.careerMp = Number(values.careerMp);
    }
    if (values.careerStarts !== undefined && typeof values.careerStarts !== 'number') {
      values.careerStarts = Number(values.careerStarts);
    }
    if (values.careerMin !== undefined && typeof values.careerMin !== 'number') {
      values.careerMin = Number(values.careerMin);
    }
    if (values.career90s !== undefined && typeof values.career90s !== 'number') {
      values.career90s = Number(values.career90s);
    }
    if (values.careerGls !== undefined && typeof values.careerGls !== 'number') {
      values.careerGls = Number(values.careerGls);
    }
    if (values.careerAst !== undefined && typeof values.careerAst !== 'number') {
      values.careerAst = Number(values.careerAst);
    }
    if (values.careerGPlusA !== undefined && typeof values.careerGPlusA !== 'number') {
      values.careerGPlusA = Number(values.careerGPlusA);
    }
    if (values.careerXg !== undefined && typeof values.careerXg !== 'number') {
      values.careerXg = Number(values.careerXg);
    }
    if (values.careerXag !== undefined && typeof values.careerXag !== 'number') {
      values.careerXag = Number(values.careerXag);
    }
    if (values.careerXgPlusXag !== undefined && typeof values.careerXgPlusXag !== 'number') {
      values.careerXgPlusXag = Number(values.careerXgPlusXag);
    }
    if (values.careerNpxg !== undefined && typeof values.careerNpxg !== 'number') {
      values.careerNpxg = Number(values.careerNpxg);
    }
    if (values.careerNpxgPlusXag !== undefined && typeof values.careerNpxgPlusXag !== 'number') {
      values.careerNpxgPlusXag = Number(values.careerNpxgPlusXag);
    }
    if (values.careerGlsPer90 !== undefined && typeof values.careerGlsPer90 !== 'number') {
      values.careerGlsPer90 = Number(values.careerGlsPer90);
    }
    if (values.careerAstPer90 !== undefined && typeof values.careerAstPer90 !== 'number') {
      values.careerAstPer90 = Number(values.careerAstPer90);
    }
    if (values.careerGPlusAPer90 !== undefined && typeof values.careerGPlusAPer90 !== 'number') {
      values.careerGPlusAPer90 = Number(values.careerGPlusAPer90);
    }
    if (values.careerXgPer90 !== undefined && typeof values.careerXgPer90 !== 'number') {
      values.careerXgPer90 = Number(values.careerXgPer90);
    }
    if (values.careerXagPer90 !== undefined && typeof values.careerXagPer90 !== 'number') {
      values.careerXagPer90 = Number(values.careerXagPer90);
    }
    if (values.careerXgPlusXagPer90 !== undefined && typeof values.careerXgPlusXagPer90 !== 'number') {
      values.careerXgPlusXagPer90 = Number(values.careerXgPlusXagPer90);
    }

    const entity = {
      ...giocatoreEntity,
      ...values,
      squadra: squadras.find(it => it.id.toString() === values.squadra?.toString()),
      watchList: watchLists.find(it => it.id.toString() === values.watchList?.toString()),
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
          role: 'GK',
          ...giocatoreEntity,
          squadra: giocatoreEntity?.squadra?.id,
          watchList: giocatoreEntity?.watchList?.id,
        };

  return (
    <div>
      <Row className="justify-content-center">
        <Col md="8">
          <h2 id="astaAppApp.giocatore.home.createOrEditLabel" data-cy="GiocatoreCreateUpdateHeading">
            Genera o modifica un Giocatore
          </h2>
        </Col>
      </Row>
      <Row className="justify-content-center">
        <Col md="8">
          {loading ? (
            <p>Loading...</p>
          ) : (
            <ValidatedForm defaultValues={defaultValues()} onSubmit={saveEntity}>
              {!isNew ? <ValidatedField name="id" required readOnly id="giocatore-id" label="ID" validate={{ required: true }} /> : null}
              <ValidatedField label="Player" id="giocatore-player" name="player" data-cy="player" type="text" />
              <ValidatedField label="Team" id="giocatore-team" name="team" data-cy="team" type="text" />
              <ValidatedField label="Season" id="giocatore-season" name="season" data-cy="season" type="text" />
              <ValidatedField label="Age" id="giocatore-age" name="age" data-cy="age" type="text" />
              <ValidatedField label="Squad" id="giocatore-squad" name="squad" data-cy="squad" type="text" />
              <ValidatedField label="Comp" id="giocatore-comp" name="comp" data-cy="comp" type="text" />
              <ValidatedField label="Country" id="giocatore-country" name="country" data-cy="country" type="text" />
              <ValidatedField label="Mp" id="giocatore-mp" name="mp" data-cy="mp" type="text" />
              <ValidatedField label="Starts" id="giocatore-starts" name="starts" data-cy="starts" type="text" />
              <ValidatedField label="Starts Pct" id="giocatore-startsPct" name="startsPct" data-cy="startsPct" type="text" />
              <ValidatedField label="Minutes" id="giocatore-minutes" name="minutes" data-cy="minutes" type="text" />
              <ValidatedField label="Nins" id="giocatore-nins" name="nins" data-cy="nins" type="text" />
              <ValidatedField label="Gls" id="giocatore-gls" name="gls" data-cy="gls" type="text" />
              <ValidatedField label="Ast" id="giocatore-ast" name="ast" data-cy="ast" type="text" />
              <ValidatedField label="G Plus A" id="giocatore-gPlusA" name="gPlusA" data-cy="gPlusA" type="text" />
              <ValidatedField label="G Pk" id="giocatore-gPk" name="gPk" data-cy="gPk" type="text" />
              <ValidatedField label="Pk" id="giocatore-pk" name="pk" data-cy="pk" type="text" />
              <ValidatedField label="Pkatt" id="giocatore-pkatt" name="pkatt" data-cy="pkatt" type="text" />
              <ValidatedField label="Crdy" id="giocatore-crdy" name="crdy" data-cy="crdy" type="text" />
              <ValidatedField label="Crdr" id="giocatore-crdr" name="crdr" data-cy="crdr" type="text" />
              <ValidatedField label="Xg" id="giocatore-xg" name="xg" data-cy="xg" type="text" />
              <ValidatedField label="Xag" id="giocatore-xag" name="xag" data-cy="xag" type="text" />
              <ValidatedField label="Npxg" id="giocatore-npxg" name="npxg" data-cy="npxg" type="text" />
              <ValidatedField label="Xg Plus Xag" id="giocatore-xgPlusXag" name="xgPlusXag" data-cy="xgPlusXag" type="text" />
              <ValidatedField label="Npxg Plus Xag" id="giocatore-npxgPlusXag" name="npxgPlusXag" data-cy="npxgPlusXag" type="text" />
              <ValidatedField label="Prgp" id="giocatore-prgp" name="prgp" data-cy="prgp" type="text" />
              <ValidatedField label="Prgc" id="giocatore-prgc" name="prgc" data-cy="prgc" type="text" />
              <ValidatedField label="Prgr" id="giocatore-prgr" name="prgr" data-cy="prgr" type="text" />
              <ValidatedField label="Gls Per 90" id="giocatore-glsPer90" name="glsPer90" data-cy="glsPer90" type="text" />
              <ValidatedField label="Ast Per 90" id="giocatore-astPer90" name="astPer90" data-cy="astPer90" type="text" />
              <ValidatedField label="G Plus A Per 90" id="giocatore-gPlusAPer90" name="gPlusAPer90" data-cy="gPlusAPer90" type="text" />
              <ValidatedField label="Xg Per 90" id="giocatore-xgPer90" name="xgPer90" data-cy="xgPer90" type="text" />
              <ValidatedField label="Xag Per 90" id="giocatore-xagPer90" name="xagPer90" data-cy="xagPer90" type="text" />
              <ValidatedField
                label="Xg Plus Xag Per 90"
                id="giocatore-xgPlusXagPer90"
                name="xgPlusXagPer90"
                data-cy="xgPlusXagPer90"
                type="text"
              />
              <ValidatedField label="Xg Diff" id="giocatore-xgDiff" name="xgDiff" data-cy="xgDiff" type="text" />
              <ValidatedField label="Xa Diff" id="giocatore-xaDiff" name="xaDiff" data-cy="xaDiff" type="text" />
              <ValidatedField label="Trend" id="giocatore-trend" name="trend" data-cy="trend" type="text" />
              <ValidatedField label="Newleague" id="giocatore-newleague" name="newleague" data-cy="newleague" type="text" />
              <ValidatedField label="Sourcefile" id="giocatore-sourcefile" name="sourcefile" data-cy="sourcefile" type="text" />
              <ValidatedField label="Role" id="giocatore-role" name="role" data-cy="role" type="select">
                {roleValues.map(role => (
                  <option value={role} key={role}>
                    {role}
                  </option>
                ))}
              </ValidatedField>
              <ValidatedField label="Career Mp" id="giocatore-careerMp" name="careerMp" data-cy="careerMp" type="text" />
              <ValidatedField label="Career Starts" id="giocatore-careerStarts" name="careerStarts" data-cy="careerStarts" type="text" />
              <ValidatedField label="Career Min" id="giocatore-careerMin" name="careerMin" data-cy="careerMin" type="text" />
              <ValidatedField label="Career 90 S" id="giocatore-career90s" name="career90s" data-cy="career90s" type="text" />
              <ValidatedField label="Career Gls" id="giocatore-careerGls" name="careerGls" data-cy="careerGls" type="text" />
              <ValidatedField label="Career Ast" id="giocatore-careerAst" name="careerAst" data-cy="careerAst" type="text" />
              <ValidatedField label="Career G Plus A" id="giocatore-careerGPlusA" name="careerGPlusA" data-cy="careerGPlusA" type="text" />
              <ValidatedField label="Career Xg" id="giocatore-careerXg" name="careerXg" data-cy="careerXg" type="text" />
              <ValidatedField label="Career Xag" id="giocatore-careerXag" name="careerXag" data-cy="careerXag" type="text" />
              <ValidatedField
                label="Career Xg Plus Xag"
                id="giocatore-careerXgPlusXag"
                name="careerXgPlusXag"
                data-cy="careerXgPlusXag"
                type="text"
              />
              <ValidatedField label="Career Npxg" id="giocatore-careerNpxg" name="careerNpxg" data-cy="careerNpxg" type="text" />
              <ValidatedField
                label="Career Npxg Plus Xag"
                id="giocatore-careerNpxgPlusXag"
                name="careerNpxgPlusXag"
                data-cy="careerNpxgPlusXag"
                type="text"
              />
              <ValidatedField
                label="Career Gls Per 90"
                id="giocatore-careerGlsPer90"
                name="careerGlsPer90"
                data-cy="careerGlsPer90"
                type="text"
              />
              <ValidatedField
                label="Career Ast Per 90"
                id="giocatore-careerAstPer90"
                name="careerAstPer90"
                data-cy="careerAstPer90"
                type="text"
              />
              <ValidatedField
                label="Career G Plus A Per 90"
                id="giocatore-careerGPlusAPer90"
                name="careerGPlusAPer90"
                data-cy="careerGPlusAPer90"
                type="text"
              />
              <ValidatedField
                label="Career Xg Per 90"
                id="giocatore-careerXgPer90"
                name="careerXgPer90"
                data-cy="careerXgPer90"
                type="text"
              />
              <ValidatedField
                label="Career Xag Per 90"
                id="giocatore-careerXagPer90"
                name="careerXagPer90"
                data-cy="careerXagPer90"
                type="text"
              />
              <ValidatedField
                label="Career Xg Plus Xag Per 90"
                id="giocatore-careerXgPlusXagPer90"
                name="careerXgPlusXagPer90"
                data-cy="careerXgPlusXagPer90"
                type="text"
              />
              <ValidatedField id="giocatore-squadra" name="squadra" data-cy="squadra" label="Squadra" type="select">
                <option value="" key="0" />
                {squadras
                  ? squadras.map(otherEntity => (
                      <option value={otherEntity.id} key={otherEntity.id}>
                        {otherEntity.id}
                      </option>
                    ))
                  : null}
              </ValidatedField>
              <ValidatedField id="giocatore-watchList" name="watchList" data-cy="watchList" label="Watch List" type="select">
                <option value="" key="0" />
                {watchLists
                  ? watchLists.map(otherEntity => (
                      <option value={otherEntity.id} key={otherEntity.id}>
                        {otherEntity.id}
                      </option>
                    ))
                  : null}
              </ValidatedField>
              <Button tag={Link} id="cancel-save" data-cy="entityCreateCancelButton" to="/giocatore" replace color="info">
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

export default GiocatoreUpdate;
