import React, { useState, useEffect } from 'react';
import { Row, Col, Card, CardBody, CardTitle, Form, FormGroup, Label, Input, Button, Table, Badge, Alert } from 'reactstrap';
import { useAppDispatch, useAppSelector } from 'app/config/store';
import { getEntities as getWatchLists } from 'app/entities/watch-list/watch-list.reducer';
import { getEntities as getSquadre } from 'app/entities/squadra/squadra.reducer';
import { getEntities as getRosters } from 'app/entities/roster/roster.reducer';
import { getEntities as getGiocatori } from 'app/entities/giocatore/giocatore.reducer';
import { getEntities as getCoaches } from 'app/entities/coach/coach.reducer';
import { updateEntity as updateRoster } from 'app/entities/roster/roster.reducer';
import { IWatchList } from 'app/shared/model/watch-list.model';
import { ISquadra } from 'app/shared/model/squadra.model';
import { IGiocatore } from 'app/shared/model/giocatore.model';
import { IRoster } from 'app/shared/model/roster.model';
import { ICoach } from 'app/shared/model/coach.model';
import dayjs from 'dayjs';
import './dashboard.scss';

const updateRosterByRole = (roster: IRoster, role: string) => {
  const updatedRoster = { ...roster };
  switch (role) {
    case 'PORTIERE':
      updatedRoster.port = (updatedRoster.port || 0) + 1;
      break;
    case 'DIFENSORE':
      updatedRoster.dif = (updatedRoster.dif || 0) + 1;
      break;
    case 'CENTROCAMPISTA':
      updatedRoster.cc = (updatedRoster.cc || 0) + 1;
      break;
    case 'ATTACCANTE':
      updatedRoster.att = (updatedRoster.att || 0) + 1;
      break;
    default:
      break;
  }
  const totalPlayers = (updatedRoster.port || 0) + (updatedRoster.dif || 0) + (updatedRoster.cc || 0) + (updatedRoster.att || 0);
  updatedRoster.full = totalPlayers >= 25;
  return updatedRoster;
};

const getRosterStats = (rosters: IRoster[], squadraId: number) => {
  const roster = rosters.find(r => r.squadra?.id === squadraId);
  if (!roster) {
    return { port: 0, dif: 0, cc: 0, att: 0, total: 0, full: false };
  }

  const port = roster.port || 0;
  const dif = roster.dif || 0;
  const cc = roster.cc || 0;
  const att = roster.att || 0;
  const total = port + dif + cc + att;

  return { port, dif, cc, att, total, full: roster.full || false };
};

export const Dashboard = () => {
  const dispatch = useAppDispatch();

  const [selectedSquadra, setSelectedSquadra] = useState<number | null>(null);
  const [selectedGiocatore, setSelectedGiocatore] = useState<number | null>(null);
  const [costo, setCosto] = useState<number | null>(null);
  const [purchaseSuccess, setPurchaseSuccess] = useState<boolean>(false);

  const watchLists = useAppSelector(state => state.watchList.entities);
  const squadre = useAppSelector(state => state.squadra.entities);
  const rosters = useAppSelector(state => state.roster.entities);
  const giocatori = useAppSelector(state => state.giocatore.entities);
  const coaches = useAppSelector(state => state.coach.entities);
  const loading = useAppSelector(
    state => state.watchList.loading || state.squadra.loading || state.roster.loading || state.giocatore.loading || state.coach.loading,
  );

  useEffect(() => {
    dispatch(getWatchLists({}));
    dispatch(getSquadre({}));
    dispatch(getRosters({}));
    dispatch(getGiocatori({}));
    dispatch(getCoaches({}));
  }, []);

  const getCurrentUserCoach = () => {
    return coaches.find(coach => coach.itsMe === true);
  };

  const getCurrentUserSquadra = () => {
    const currentCoach = getCurrentUserCoach();
    return currentCoach?.squadra || null;
  };

  const getCurrentUserWatchList = () => {
    const currentSquadra = getCurrentUserSquadra();
    if (!currentSquadra) return null;
    return watchLists.find(wl => wl.squadra && wl.squadra.id === currentSquadra.id);
  };

  const handlePlayerPurchase = async () => {
    if (!selectedSquadra || !selectedGiocatore || costo === null || costo <= 0) {
      return;
    }

    const selectedPlayer = giocatori.find(g => g.id === selectedGiocatore);
    const targetRoster = rosters.find(r => r.squadra?.id === selectedSquadra);

    if (!selectedPlayer || !targetRoster) {
      return;
    }

    const updatedRoster = updateRosterByRole(targetRoster, selectedPlayer.role || '');

    try {
      await dispatch(updateRoster(updatedRoster));
      setPurchaseSuccess(true);
      setTimeout(() => setPurchaseSuccess(false), 3000);

      // Reset form
      setSelectedGiocatore(null);
      setCosto(null);

      // Ricarica i dati per vedere le modifiche
      dispatch(getRosters({}));
    } catch (error) {
      console.error("Errore nell'aggiornamento del roster:", error);
    }
  };

  const currentWatchList = getCurrentUserWatchList();
  const currentUserSquadra = getCurrentUserSquadra();
  const selectedSquadraObj = squadre.find(s => s.id === selectedSquadra);
  const rosterStats = selectedSquadra ? getRosterStats(rosters, selectedSquadra) : null;
  const currentUserRosterStats = currentUserSquadra ? getRosterStats(rosters, currentUserSquadra.id) : null;

  return (
    <div className="dashboard-container">
      <h2>Dashboard Fantacalcio</h2>

      {purchaseSuccess && (
        <Alert color="success" className="mb-3">
          Giocatore acquistato con successo! Roster aggiornato.
        </Alert>
      )}

      <Row>
        <Col md="6">
          <Card className="mb-4">
            <CardBody>
              <CardTitle tag="h4">La Mia Squadra</CardTitle>

              {currentUserSquadra ? (
                <>
                  <div className="roster-info mb-3">
                    <h5>Squadra: {currentUserSquadra.nome}</h5>
                    <Badge color={currentUserRosterStats?.full ? 'danger' : 'success'} className="me-2">
                      {currentUserRosterStats?.full ? 'Completo' : 'Disponibile'}
                    </Badge>
                    <div className="roster-stats mt-2">
                      <small>
                        <strong>Portieri:</strong> {currentUserRosterStats?.port || 0}/3 |<strong> Difensori:</strong>{' '}
                        {currentUserRosterStats?.dif || 0}/8 |<strong> Centrocampisti:</strong> {currentUserRosterStats?.cc || 0}/8 |
                        <strong> Attaccanti:</strong> {currentUserRosterStats?.att || 0}/6
                      </small>
                      <div>
                        <strong>Totale: {currentUserRosterStats?.total || 0}/25</strong>
                      </div>
                    </div>
                  </div>

                  {currentWatchList && (
                    <div className="watchlist-info">
                      <h6>Watchlist: {currentWatchList.version || 'N/A'}</h6>
                      <small>Aggiornata: {currentWatchList.date ? dayjs(currentWatchList.date).format('DD/MM/YYYY HH:mm') : 'N/A'}</small>
                    </div>
                  )}

                  {!currentWatchList && <Alert color="info">Nessuna watchlist trovata per la tua squadra.</Alert>}
                </>
              ) : (
                <Alert color="warning">Nessuna squadra associata al tuo profilo. Assicurati di avere un Coach con itsMe = true.</Alert>
              )}
            </CardBody>
          </Card>
        </Col>

        <Col md="6">
          <Card className="mb-4">
            <CardBody>
              <CardTitle tag="h4">Acquisto Giocatori</CardTitle>

              <Form>
                <FormGroup>
                  <Label for="squadra-acquirente">Squadra Acquirente:</Label>
                  <Input
                    type="select"
                    id="squadra-acquirente"
                    value={selectedSquadra || ''}
                    onChange={e => setSelectedSquadra(e.target.value ? parseInt(e.target.value, 10) : null)}
                  >
                    <option value="">-- Seleziona squadra acquirente --</option>
                    {squadre.map(squadra => (
                      <option key={squadra.id} value={squadra.id}>
                        {squadra.nome}
                      </option>
                    ))}
                  </Input>
                </FormGroup>

                <FormGroup>
                  <Label for="giocatore-select">Seleziona Giocatore:</Label>
                  <Input
                    type="select"
                    id="giocatore-select"
                    value={selectedGiocatore || ''}
                    onChange={e => setSelectedGiocatore(e.target.value ? parseInt(e.target.value, 10) : null)}
                    disabled={!selectedSquadra}
                  >
                    <option value="">-- Seleziona un giocatore --</option>
                    {giocatori.map(giocatore => (
                      <option key={giocatore.id} value={giocatore.id}>
                        {giocatore.player} - {giocatore.team} ({giocatore.role || 'N/A'})
                      </option>
                    ))}
                  </Input>
                </FormGroup>

                <FormGroup>
                  <Label for="costo">Costo (€):</Label>
                  <Input
                    type="number"
                    id="costo"
                    value={costo || ''}
                    onChange={e => setCosto(e.target.value ? parseInt(e.target.value, 10) : null)}
                    placeholder="Inserisci il costo"
                    min="1"
                    disabled={!selectedGiocatore}
                  />
                </FormGroup>

                <Button
                  color="primary"
                  onClick={handlePlayerPurchase}
                  disabled={!selectedSquadra || !selectedGiocatore || !costo || costo <= 0 || loading}
                  type="button"
                >
                  {loading ? 'Elaborazione...' : 'Conferma Acquisto'}
                </Button>
              </Form>
            </CardBody>
          </Card>
        </Col>
      </Row>

      <Row>
        <Col md="12">
          <Card>
            <CardBody>
              <CardTitle tag="h4">Riepilogo Roster Squadre</CardTitle>

              <Table responsive striped>
                <thead>
                  <tr>
                    <th>Squadra</th>
                    <th>Portieri</th>
                    <th>Difensori</th>
                    <th>Centrocampisti</th>
                    <th>Attaccanti</th>
                    <th>Totale</th>
                    <th>Stato</th>
                  </tr>
                </thead>
                <tbody>
                  {squadre.map(squadra => {
                    const stats = getRosterStats(rosters, squadra.id);
                    return (
                      <tr key={squadra.id}>
                        <td>
                          <strong>{squadra.nome}</strong>
                        </td>
                        <td>{stats.port}/3</td>
                        <td>{stats.dif}/8</td>
                        <td>{stats.cc}/8</td>
                        <td>{stats.att}/6</td>
                        <td>
                          <strong>{stats.total}/25</strong>
                        </td>
                        <td>
                          <Badge color={stats.full ? 'danger' : 'success'}>{stats.full ? 'Completo' : 'Disponibile'}</Badge>
                        </td>
                      </tr>
                    );
                  })}
                </tbody>
              </Table>
            </CardBody>
          </Card>
        </Col>
      </Row>
    </div>
  );
};

export default Dashboard;
