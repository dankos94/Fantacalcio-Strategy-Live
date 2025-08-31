import React, { useEffect, useState } from 'react';
import { Link, useLocation, useNavigate } from 'react-router-dom';
import { Button, Table } from 'reactstrap';
import { getSortState } from 'react-jhipster';
import { FontAwesomeIcon } from '@fortawesome/react-fontawesome';
import { faSort, faSortDown, faSortUp } from '@fortawesome/free-solid-svg-icons';
import { ASC, DESC } from 'app/shared/util/pagination.constants';
import { overrideSortStateWithQueryParams } from 'app/shared/util/entity-utils';
import { useAppDispatch, useAppSelector } from 'app/config/store';

import { getEntities } from './giocatore.reducer';

export const Giocatore = () => {
  const dispatch = useAppDispatch();

  const pageLocation = useLocation();
  const navigate = useNavigate();

  const [sortState, setSortState] = useState(overrideSortStateWithQueryParams(getSortState(pageLocation, 'id'), pageLocation.search));

  const giocatoreList = useAppSelector(state => state.giocatore.entities);
  const loading = useAppSelector(state => state.giocatore.loading);

  const getAllEntities = () => {
    dispatch(
      getEntities({
        sort: `${sortState.sort},${sortState.order}`,
      }),
    );
  };

  const sortEntities = () => {
    getAllEntities();
    const endURL = `?sort=${sortState.sort},${sortState.order}`;
    if (pageLocation.search !== endURL) {
      navigate(`${pageLocation.pathname}${endURL}`);
    }
  };

  useEffect(() => {
    sortEntities();
  }, [sortState.order, sortState.sort]);

  const sort = p => () => {
    setSortState({
      ...sortState,
      order: sortState.order === ASC ? DESC : ASC,
      sort: p,
    });
  };

  const handleSyncList = () => {
    sortEntities();
  };

  const getSortIconByFieldName = (fieldName: string) => {
    const sortFieldName = sortState.sort;
    const order = sortState.order;
    if (sortFieldName !== fieldName) {
      return faSort;
    }
    return order === ASC ? faSortUp : faSortDown;
  };

  return (
    <div>
      <h2 id="giocatore-heading" data-cy="GiocatoreHeading">
        Giocatores
        <div className="d-flex justify-content-end">
          <Button className="me-2" color="info" onClick={handleSyncList} disabled={loading}>
            <FontAwesomeIcon icon="sync" spin={loading} /> Refresh list
          </Button>
          <Link to="/giocatore/new" className="btn btn-primary jh-create-entity" id="jh-create-entity" data-cy="entityCreateButton">
            <FontAwesomeIcon icon="plus" />
            &nbsp; Genera un nuovo Giocatore
          </Link>
        </div>
      </h2>
      <div className="table-responsive">
        {giocatoreList && giocatoreList.length > 0 ? (
          <Table responsive>
            <thead>
              <tr>
                <th className="hand" onClick={sort('id')}>
                  ID <FontAwesomeIcon icon={getSortIconByFieldName('id')} />
                </th>
                <th className="hand" onClick={sort('player')}>
                  Player <FontAwesomeIcon icon={getSortIconByFieldName('player')} />
                </th>
                <th className="hand" onClick={sort('team')}>
                  Team <FontAwesomeIcon icon={getSortIconByFieldName('team')} />
                </th>
                <th className="hand" onClick={sort('season')}>
                  Season <FontAwesomeIcon icon={getSortIconByFieldName('season')} />
                </th>
                <th className="hand" onClick={sort('age')}>
                  Age <FontAwesomeIcon icon={getSortIconByFieldName('age')} />
                </th>
                <th className="hand" onClick={sort('squad')}>
                  Squad <FontAwesomeIcon icon={getSortIconByFieldName('squad')} />
                </th>
                <th className="hand" onClick={sort('comp')}>
                  Comp <FontAwesomeIcon icon={getSortIconByFieldName('comp')} />
                </th>
                <th className="hand" onClick={sort('country')}>
                  Country <FontAwesomeIcon icon={getSortIconByFieldName('country')} />
                </th>
                <th className="hand" onClick={sort('mp')}>
                  Mp <FontAwesomeIcon icon={getSortIconByFieldName('mp')} />
                </th>
                <th className="hand" onClick={sort('starts')}>
                  Starts <FontAwesomeIcon icon={getSortIconByFieldName('starts')} />
                </th>
                <th className="hand" onClick={sort('startsPct')}>
                  Starts Pct <FontAwesomeIcon icon={getSortIconByFieldName('startsPct')} />
                </th>
                <th className="hand" onClick={sort('minutes')}>
                  Minutes <FontAwesomeIcon icon={getSortIconByFieldName('minutes')} />
                </th>
                <th className="hand" onClick={sort('nins')}>
                  Nins <FontAwesomeIcon icon={getSortIconByFieldName('nins')} />
                </th>
                <th className="hand" onClick={sort('gls')}>
                  Gls <FontAwesomeIcon icon={getSortIconByFieldName('gls')} />
                </th>
                <th className="hand" onClick={sort('ast')}>
                  Ast <FontAwesomeIcon icon={getSortIconByFieldName('ast')} />
                </th>
                <th className="hand" onClick={sort('gPlusA')}>
                  G Plus A <FontAwesomeIcon icon={getSortIconByFieldName('gPlusA')} />
                </th>
                <th className="hand" onClick={sort('gPk')}>
                  G Pk <FontAwesomeIcon icon={getSortIconByFieldName('gPk')} />
                </th>
                <th className="hand" onClick={sort('pk')}>
                  Pk <FontAwesomeIcon icon={getSortIconByFieldName('pk')} />
                </th>
                <th className="hand" onClick={sort('pkatt')}>
                  Pkatt <FontAwesomeIcon icon={getSortIconByFieldName('pkatt')} />
                </th>
                <th className="hand" onClick={sort('crdy')}>
                  Crdy <FontAwesomeIcon icon={getSortIconByFieldName('crdy')} />
                </th>
                <th className="hand" onClick={sort('crdr')}>
                  Crdr <FontAwesomeIcon icon={getSortIconByFieldName('crdr')} />
                </th>
                <th className="hand" onClick={sort('xg')}>
                  Xg <FontAwesomeIcon icon={getSortIconByFieldName('xg')} />
                </th>
                <th className="hand" onClick={sort('xag')}>
                  Xag <FontAwesomeIcon icon={getSortIconByFieldName('xag')} />
                </th>
                <th className="hand" onClick={sort('npxg')}>
                  Npxg <FontAwesomeIcon icon={getSortIconByFieldName('npxg')} />
                </th>
                <th className="hand" onClick={sort('xgPlusXag')}>
                  Xg Plus Xag <FontAwesomeIcon icon={getSortIconByFieldName('xgPlusXag')} />
                </th>
                <th className="hand" onClick={sort('npxgPlusXag')}>
                  Npxg Plus Xag <FontAwesomeIcon icon={getSortIconByFieldName('npxgPlusXag')} />
                </th>
                <th className="hand" onClick={sort('prgp')}>
                  Prgp <FontAwesomeIcon icon={getSortIconByFieldName('prgp')} />
                </th>
                <th className="hand" onClick={sort('prgc')}>
                  Prgc <FontAwesomeIcon icon={getSortIconByFieldName('prgc')} />
                </th>
                <th className="hand" onClick={sort('prgr')}>
                  Prgr <FontAwesomeIcon icon={getSortIconByFieldName('prgr')} />
                </th>
                <th className="hand" onClick={sort('glsPer90')}>
                  Gls Per 90 <FontAwesomeIcon icon={getSortIconByFieldName('glsPer90')} />
                </th>
                <th className="hand" onClick={sort('astPer90')}>
                  Ast Per 90 <FontAwesomeIcon icon={getSortIconByFieldName('astPer90')} />
                </th>
                <th className="hand" onClick={sort('gPlusAPer90')}>
                  G Plus A Per 90 <FontAwesomeIcon icon={getSortIconByFieldName('gPlusAPer90')} />
                </th>
                <th className="hand" onClick={sort('xgPer90')}>
                  Xg Per 90 <FontAwesomeIcon icon={getSortIconByFieldName('xgPer90')} />
                </th>
                <th className="hand" onClick={sort('xagPer90')}>
                  Xag Per 90 <FontAwesomeIcon icon={getSortIconByFieldName('xagPer90')} />
                </th>
                <th className="hand" onClick={sort('xgPlusXagPer90')}>
                  Xg Plus Xag Per 90 <FontAwesomeIcon icon={getSortIconByFieldName('xgPlusXagPer90')} />
                </th>
                <th className="hand" onClick={sort('xgDiff')}>
                  Xg Diff <FontAwesomeIcon icon={getSortIconByFieldName('xgDiff')} />
                </th>
                <th className="hand" onClick={sort('xaDiff')}>
                  Xa Diff <FontAwesomeIcon icon={getSortIconByFieldName('xaDiff')} />
                </th>
                <th className="hand" onClick={sort('trend')}>
                  Trend <FontAwesomeIcon icon={getSortIconByFieldName('trend')} />
                </th>
                <th className="hand" onClick={sort('newleague')}>
                  Newleague <FontAwesomeIcon icon={getSortIconByFieldName('newleague')} />
                </th>
                <th className="hand" onClick={sort('sourcefile')}>
                  Sourcefile <FontAwesomeIcon icon={getSortIconByFieldName('sourcefile')} />
                </th>
                <th className="hand" onClick={sort('role')}>
                  Role <FontAwesomeIcon icon={getSortIconByFieldName('role')} />
                </th>
                <th className="hand" onClick={sort('careerMp')}>
                  Career Mp <FontAwesomeIcon icon={getSortIconByFieldName('careerMp')} />
                </th>
                <th className="hand" onClick={sort('careerStarts')}>
                  Career Starts <FontAwesomeIcon icon={getSortIconByFieldName('careerStarts')} />
                </th>
                <th className="hand" onClick={sort('careerMin')}>
                  Career Min <FontAwesomeIcon icon={getSortIconByFieldName('careerMin')} />
                </th>
                <th className="hand" onClick={sort('career90s')}>
                  Career 90 S <FontAwesomeIcon icon={getSortIconByFieldName('career90s')} />
                </th>
                <th className="hand" onClick={sort('careerGls')}>
                  Career Gls <FontAwesomeIcon icon={getSortIconByFieldName('careerGls')} />
                </th>
                <th className="hand" onClick={sort('careerAst')}>
                  Career Ast <FontAwesomeIcon icon={getSortIconByFieldName('careerAst')} />
                </th>
                <th className="hand" onClick={sort('careerGPlusA')}>
                  Career G Plus A <FontAwesomeIcon icon={getSortIconByFieldName('careerGPlusA')} />
                </th>
                <th className="hand" onClick={sort('careerXg')}>
                  Career Xg <FontAwesomeIcon icon={getSortIconByFieldName('careerXg')} />
                </th>
                <th className="hand" onClick={sort('careerXag')}>
                  Career Xag <FontAwesomeIcon icon={getSortIconByFieldName('careerXag')} />
                </th>
                <th className="hand" onClick={sort('careerXgPlusXag')}>
                  Career Xg Plus Xag <FontAwesomeIcon icon={getSortIconByFieldName('careerXgPlusXag')} />
                </th>
                <th className="hand" onClick={sort('careerNpxg')}>
                  Career Npxg <FontAwesomeIcon icon={getSortIconByFieldName('careerNpxg')} />
                </th>
                <th className="hand" onClick={sort('careerNpxgPlusXag')}>
                  Career Npxg Plus Xag <FontAwesomeIcon icon={getSortIconByFieldName('careerNpxgPlusXag')} />
                </th>
                <th className="hand" onClick={sort('careerGlsPer90')}>
                  Career Gls Per 90 <FontAwesomeIcon icon={getSortIconByFieldName('careerGlsPer90')} />
                </th>
                <th className="hand" onClick={sort('careerAstPer90')}>
                  Career Ast Per 90 <FontAwesomeIcon icon={getSortIconByFieldName('careerAstPer90')} />
                </th>
                <th className="hand" onClick={sort('careerGPlusAPer90')}>
                  Career G Plus A Per 90 <FontAwesomeIcon icon={getSortIconByFieldName('careerGPlusAPer90')} />
                </th>
                <th className="hand" onClick={sort('careerXgPer90')}>
                  Career Xg Per 90 <FontAwesomeIcon icon={getSortIconByFieldName('careerXgPer90')} />
                </th>
                <th className="hand" onClick={sort('careerXagPer90')}>
                  Career Xag Per 90 <FontAwesomeIcon icon={getSortIconByFieldName('careerXagPer90')} />
                </th>
                <th className="hand" onClick={sort('careerXgPlusXagPer90')}>
                  Career Xg Plus Xag Per 90 <FontAwesomeIcon icon={getSortIconByFieldName('careerXgPlusXagPer90')} />
                </th>
                <th>
                  Squadra <FontAwesomeIcon icon="sort" />
                </th>
                <th>
                  Watch List <FontAwesomeIcon icon="sort" />
                </th>
                <th />
              </tr>
            </thead>
            <tbody>
              {giocatoreList.map((giocatore, i) => (
                <tr key={`entity-${i}`} data-cy="entityTable">
                  <td>
                    <Button tag={Link} to={`/giocatore/${giocatore.id}`} color="link" size="sm">
                      {giocatore.id}
                    </Button>
                  </td>
                  <td>{giocatore.player}</td>
                  <td>{giocatore.team}</td>
                  <td>{giocatore.season}</td>
                  <td>{giocatore.age}</td>
                  <td>{giocatore.squad}</td>
                  <td>{giocatore.comp}</td>
                  <td>{giocatore.country}</td>
                  <td>{giocatore.mp}</td>
                  <td>{giocatore.starts}</td>
                  <td>{giocatore.startsPct}</td>
                  <td>{giocatore.minutes}</td>
                  <td>{giocatore.nins}</td>
                  <td>{giocatore.gls}</td>
                  <td>{giocatore.ast}</td>
                  <td>{giocatore.gPlusA}</td>
                  <td>{giocatore.gPk}</td>
                  <td>{giocatore.pk}</td>
                  <td>{giocatore.pkatt}</td>
                  <td>{giocatore.crdy}</td>
                  <td>{giocatore.crdr}</td>
                  <td>{giocatore.xg}</td>
                  <td>{giocatore.xag}</td>
                  <td>{giocatore.npxg}</td>
                  <td>{giocatore.xgPlusXag}</td>
                  <td>{giocatore.npxgPlusXag}</td>
                  <td>{giocatore.prgp}</td>
                  <td>{giocatore.prgc}</td>
                  <td>{giocatore.prgr}</td>
                  <td>{giocatore.glsPer90}</td>
                  <td>{giocatore.astPer90}</td>
                  <td>{giocatore.gPlusAPer90}</td>
                  <td>{giocatore.xgPer90}</td>
                  <td>{giocatore.xagPer90}</td>
                  <td>{giocatore.xgPlusXagPer90}</td>
                  <td>{giocatore.xgDiff}</td>
                  <td>{giocatore.xaDiff}</td>
                  <td>{giocatore.trend}</td>
                  <td>{giocatore.newleague}</td>
                  <td>{giocatore.sourcefile}</td>
                  <td>{giocatore.role}</td>
                  <td>{giocatore.careerMp}</td>
                  <td>{giocatore.careerStarts}</td>
                  <td>{giocatore.careerMin}</td>
                  <td>{giocatore.career90s}</td>
                  <td>{giocatore.careerGls}</td>
                  <td>{giocatore.careerAst}</td>
                  <td>{giocatore.careerGPlusA}</td>
                  <td>{giocatore.careerXg}</td>
                  <td>{giocatore.careerXag}</td>
                  <td>{giocatore.careerXgPlusXag}</td>
                  <td>{giocatore.careerNpxg}</td>
                  <td>{giocatore.careerNpxgPlusXag}</td>
                  <td>{giocatore.careerGlsPer90}</td>
                  <td>{giocatore.careerAstPer90}</td>
                  <td>{giocatore.careerGPlusAPer90}</td>
                  <td>{giocatore.careerXgPer90}</td>
                  <td>{giocatore.careerXagPer90}</td>
                  <td>{giocatore.careerXgPlusXagPer90}</td>
                  <td>{giocatore.squadra ? <Link to={`/squadra/${giocatore.squadra.id}`}>{giocatore.squadra.id}</Link> : ''}</td>
                  <td>{giocatore.watchList ? <Link to={`/watch-list/${giocatore.watchList.id}`}>{giocatore.watchList.id}</Link> : ''}</td>
                  <td className="text-end">
                    <div className="btn-group flex-btn-group-container">
                      <Button tag={Link} to={`/giocatore/${giocatore.id}`} color="info" size="sm" data-cy="entityDetailsButton">
                        <FontAwesomeIcon icon="eye" /> <span className="d-none d-md-inline">Visualizza</span>
                      </Button>
                      <Button tag={Link} to={`/giocatore/${giocatore.id}/edit`} color="primary" size="sm" data-cy="entityEditButton">
                        <FontAwesomeIcon icon="pencil-alt" /> <span className="d-none d-md-inline">Modifica</span>
                      </Button>
                      <Button
                        onClick={() => (window.location.href = `/giocatore/${giocatore.id}/delete`)}
                        color="danger"
                        size="sm"
                        data-cy="entityDeleteButton"
                      >
                        <FontAwesomeIcon icon="trash" /> <span className="d-none d-md-inline">Elimina</span>
                      </Button>
                    </div>
                  </td>
                </tr>
              ))}
            </tbody>
          </Table>
        ) : (
          !loading && <div className="alert alert-warning">No Giocatores found</div>
        )}
      </div>
    </div>
  );
};

export default Giocatore;
