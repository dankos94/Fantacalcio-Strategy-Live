"""
Libreria per la gestione dei dati calcistici estratti dal dataset
``ESPN Soccer Data`` di Excel4Soccer su Kaggle. Il dataset contiene più
directory con file CSV che fanno riferimento fra loro tramite chiavi
identificative (per esempio ``eventId``, ``teamId`` e ``athleteId``).  Questa
libreria fornisce un'interfaccia di alto livello per leggere i vari
file, esplorare le informazioni e creare DataFrame collegati, adatti
all'analisi delle performance dei giocatori di Serie A o di qualsiasi
altro campionato presente.

La struttura del dataset include:

* ``base_data`` – file CSV principali con fixture (calendario delle
  partite), informazioni su squadre, giocatori, campionati, stadi,
  statistiche di squadra, classifiche e rose attuali.
* ``commentary_data`` – commenti testuali per ogni partita, indicizzati
  da ``eventId`` e ordinati da ``commentaryOrder`` con riferimenti
  eventuali a ``playId``.  I file sono organizzati per stagione e
  campionato.
* ``keyEvents_data`` – eventi chiave di ciascun incontro (gol,
  cartellini, sostituzioni, ecc.), indicizzati da ``eventId`` e
  ``playId``.  Contengono anche il tempo di gioco, la squadra e il
  giocatore coinvolto.
* ``lineup_data`` – formazioni di partenza e sostituzioni, indicizzate
  da ``eventId`` e ``teamId``; contengono l'identificativo di ogni
  giocatore schierato, il ruolo, il numero di maglia e gli orari di
  ingresso/uscita dal campo.
* ``playerStats_data`` – statistiche per giocatore e partita (tiri,
  passaggi, contrasti, ecc.), indicizzate da ``eventId``, ``teamId`` e
  ``athleteId``.
* ``plays_data`` – cronologia dettagliata di tutte le azioni di gioco,
  indicizzate da ``eventId`` e ``playId``.

Lettura e unione dei file
-------------------------

Gli analisti che lavorano con dati ESPN solitamente usano librerie come
``soccerdata`` che forniscono DataFrame Pandas direttamente dal
servizio JSON di ESPN; questa libreria, ad esempio, restituisce
DataFrame come il calendario delle partite (schedule) e le formazioni
con un'interfaccia pulita【587213457719709†L91-L100】【587213457719709†L172-L186】.  Tuttavia, il dataset
Excel4Soccer organizza gli stessi dati in file CSV locali e richiede
una gestione manuale delle chiavi.  La classe ``ESPNDataRepository``
implementata di seguito fornisce metodi CRUD per caricare i file in
DataFrame Pandas, filtrare per campionato/stagione e accedere alle
informazioni correlate.

Note sull'utilizzo:
-------------------

* Tutti i percorsi dovranno puntare alla cartella in cui avete
  estratto l'archivio Kaggle (ad esempio ``/path/to/espn-soccer-data``).
* Poiché alcuni file sono suddivisi per stagione e campionato (es.
  ``commentary_2024_ITA_SERIEA.csv``), la libreria cerca questi
  pattern in base alla stagione e alla sigla del campionato.
* La libreria non modifica i file originali; eventuali aggiornamenti
  avvengono sui DataFrame in memoria.
"""

from __future__ import annotations

import os
import re
import zipfile
from pathlib import Path
from typing import Iterable, Optional, List, Tuple, Dict

import pandas as pd


class ESPNDataRepository:
    """Repository per accedere ai dati ESPN salvati in locale.

    Parametri
    ---------
    root_dir : str or Path
        Percorso alla cartella principale del dataset scaricato da Kaggle.
    autocache : bool, optional
        Se ``True``, le tabelle base vengono memorizzate in cache
        internamente al primo accesso per evitare letture ripetute.

    Esempi
    -------

    >>> repo = ESPNDataRepository("/dati/espn-soccer-data")
    >>> fixtures = repo.fixtures()  # carica il calendario (base_data/fixtures.csv)
    >>> serie_a_matches = fixtures[fixtures["leagueId"] == "ita.1"]
    >>> match_id = serie_a_matches.iloc[0]["eventId"]
    >>> commentary = repo.commentary_for_event(match_id)
    >>> player_stats = repo.player_stats_for_event(match_id)

    I DataFrame ritornati possono poi essere uniti tramite chiavi
    comuni (``eventId``, ``teamId``, ``athleteId``) per arricchire
    l'analisi, ad esempio combinando le statistiche dei giocatori con
    l'informazione anagrafica contenuta in ``players.csv``.
    """

    # nomi delle tabelle di base presenti in base_data
    BASE_TABLES = {
        "fixtures": "fixtures.csv",
        "teams": "teams.csv",
        "players": "players.csv",
        "leagues": "leagues.csv",
        "venues": "venues.csv",
        "team_stats": "teamStats.csv",
        "standings": "standings.csv",
        "team_roster": "teamRoster.csv",
        "status": "status.csv",
    }

    def __init__(self, root_dir: Path | str, autocache: bool = True) -> None:
        self.root_dir = Path(root_dir)
        if not self.root_dir.exists():
            raise FileNotFoundError(f"La directory {self.root_dir} non esiste")
        self.autocache = autocache
        self._cache: Dict[str, pd.DataFrame] = {}

    # ------------------------------------------------------------------
    # Metodi per caricare tabelle di base
    def _load_base_table(self, name: str) -> pd.DataFrame:
        """Carica una tabella CSV dalla cartella base_data.

        Se ``autocache`` è attivo, la tabella viene memorizzata internamente.

        Parametri
        ----------
        name : str
            Nome logico della tabella (ad esempio ``"fixtures"`` o ``"players"``).

        Ritorna
        -------
        pandas.DataFrame
            La tabella richiesta.
        """
        if name in self._cache:
            return self._cache[name].copy()
        fname = self.BASE_TABLES.get(name)
        if fname is None:
            raise KeyError(f"Tabella base sconosciuta: {name}")
        path = self.root_dir / "base_data" / fname
        if not path.exists():
            raise FileNotFoundError(f"File non trovato: {path}")
        df = pd.read_csv(path)
        if self.autocache:
            self._cache[name] = df
        return df.copy()

    def fixtures(self) -> pd.DataFrame:
        """Restituisce il DataFrame delle partite (calendario).

        Ogni riga rappresenta un incontro, indicizzato da ``eventId`` e contiene
        informazioni come data/ora, squadre coinvolte, lega e stagione.  Queste
        informazioni sono analoghe al calendario fornito dalla libreria
        ``soccerdata`` tramite ``read_schedule``【587213457719709†L132-L142】.
        """
        return self._load_base_table("fixtures")

    def teams(self) -> pd.DataFrame:
        """Restituisce il DataFrame con le informazioni sulle squadre.

        La tabella è indicizzata da ``teamId`` e include nomi, città,
        abbreviations e informazioni sulla lega di appartenenza.
        """
        return self._load_base_table("teams")

    def players(self) -> pd.DataFrame:
        """Restituisce il DataFrame con le informazioni sui giocatori.

        Ogni record è identificato da ``athleteId`` e comprende nome,
        cognome, nazionalità, data di nascita e altri attributi
        anagrafici.
        """
        return self._load_base_table("players")

    def leagues(self) -> pd.DataFrame:
        """Restituisce l'elenco dei campionati e stagioni presenti.

        Contiene ``leagueId``, ``season``, ``seasonType`` e un campo
        ``midsizeName`` che rappresenta il nome leggibile della lega.
        """
        return self._load_base_table("leagues")

    def venues(self) -> pd.DataFrame:
        """Restituisce l'elenco degli stadi (luoghi).

        Ogni riga è indicizzata da ``venueId`` e contiene nome,
        capienza e città dello stadio.
        """
        return self._load_base_table("venues")

    def team_stats(self) -> pd.DataFrame:
        """Restituisce le statistiche di squadra per partita.

        La tabella è indicizzata da ``eventId`` e ``teamId`` e contiene
        statistiche aggregate (tiri, possesso palla, ecc.), simili a
        quelle disponibili tramite ``read_matchsheet`` di ``soccerdata``
        【587213457719709†L154-L168】.
        """
        return self._load_base_table("team_stats")

    def standings(self) -> pd.DataFrame:
        """Restituisce la classifica aggiornata per ogni lega e stagione.

        Indicizzata da ``seasonType`` e ``teamId``; utile per capire la
        posizione corrente di una squadra in Serie A o in altri campionati.
        """
        return self._load_base_table("standings")

    def team_roster(self) -> pd.DataFrame:
        """Restituisce la rosa attuale delle squadre.

        Indicizzata da ``seasonType`` e ``teamId``.  Per stagioni precedenti al
        2024-2025 la rosa non è disponibile nel dataset.
        """
        return self._load_base_table("team_roster")

    def status(self) -> pd.DataFrame:
        """Restituisce lo stato della partita.

        Contiene informazioni sul risultato finale (vittoria/sconfitta/pareggio)
        e altri indicatori di esito.
        """
        return self._load_base_table("status")

    # ------------------------------------------------------------------
    # Caricamento dei file specifici per stagione e campionato
    def _load_files_by_pattern(self, subfolder: str, pattern: str) -> pd.DataFrame:
        """Carica e concatena tutti i file in ``subfolder`` che corrispondono al pattern.

        Il pattern è una stringa che deve trovarsi all'inizio del nome del file,
        ad esempio ``commentary_2024_ITA``.  I file possono essere CSV
        o ZIP contenenti uno o più CSV.  Tutti i CSV vengono letti e
        concatenati insieme.

        Parametri
        ----------
        subfolder : str
            Nome della sottocartella (es. ``"commentary_data"``).
        pattern : str
            Prefisso dei file da cercare.

        Ritorna
        -------
        pandas.DataFrame
            Concatenazione di tutti i file trovati; se nessun file
            corrisponde viene restituito un DataFrame vuoto.
        """
        folder = self.root_dir / subfolder
        if not folder.exists():
            raise FileNotFoundError(f"Cartella non trovata: {folder}")
        data_frames: List[pd.DataFrame] = []
        for fname in os.listdir(folder):
            if not fname.startswith(pattern):
                continue
            file_path = folder / fname
            # CSV diretto
            if fname.lower().endswith(".csv"):
                df = pd.read_csv(file_path)
                data_frames.append(df)
            # Archivio ZIP
            elif fname.lower().endswith(".zip"):
                with zipfile.ZipFile(file_path) as zf:
                    for inner_name in zf.namelist():
                        if not inner_name.lower().endswith(".csv"):
                            continue
                        with zf.open(inner_name) as inner:
                            df = pd.read_csv(inner)
                            data_frames.append(df)
        if data_frames:
            return pd.concat(data_frames, ignore_index=True)
        # nessun file trovato
        return pd.DataFrame()

    # PATCH — sostituisci in espn_data_reader.py

    # helper (mettilo nella classe, vicino alle altre utility)
    def _id_str(self, x):
        try:
            return str(int(float(x)))
        except Exception:
            return str(x)

    # sostituisci completamente _derive_season_league_code
    def _derive_season_league_code(self, event_id):
        import pandas as pd

        eid = self._id_str(event_id)
        fx = self.fixtures().copy()

        # cerca l'evento in modo robusto (string/numero)
        candidates = [c for c in ["eventId", "eventid", "gameId", "id", "matchId"] if c in fx.columns]
        row = None
        for c in candidates:
            hit = fx.loc[fx[c].map(self._id_str) == eid]
            if not hit.empty:
                row = hit.iloc[0]
                break
        if row is None:
            raise KeyError(f"Evento {event_id} non presente in fixtures")

        # season Year
        if "seasonYear" in fx.columns:
            season_year = int(self._id_str(row["seasonYear"]))
        elif "season" in fx.columns:
            sv = str(row["season"])
            season_year = int(sv.split("-")[0])  # gestisce '2024' o '2024-2025'
        else:
            season_year = int(pd.to_datetime(row["date"]).year)

        # league code (accetta leagueId/midsizeName/league)
        for c in ["leagueId", "midsizeName", "league", "league_code"]:
            if c in fx.columns:
                league_code = str(row[c])
                break
        else:
            league_code = None

        return season_year, league_code


    # ------------------------------------------------------------------
    # Metodi pubblici per caricare file di dettaglio
    def commentary_for_event(self, event_id: int) -> pd.DataFrame:
        """Restituisce il commento testuale per una partita.

        Il metodo identifica automaticamente file di commentary relativi
        alla stagione e alla lega della partita e filtra il DataFrame
        risultante per l'``eventId`` specificato.  Le colonne tipiche
        includono ``commentaryOrder``, ``playId``, testo del commento,
        orario di gioco e (quando disponibile) identità di squadra e
        giocatore coinvolti.

        Ritorna un DataFrame eventualmente vuoto se non ci sono
        commenti.  Per ulteriori dettagli sui campi disponibili si
        possono confrontare i DataFrame restituiti con i campi
        ``lineup`` nel pacchetto ``soccerdata``【587213457719709†L172-L186】.
        """
        season_year, league_code = self._derive_season_league_code(event_id)
        df = self._load_files_by_pattern("commentary_data", f"commentary_{season_year}_{league_code}")
        if not df.empty and "eventId" in df.columns:
            df = df[df["eventId"] == event_id]
        return df.reset_index(drop=True)

    def key_events_for_event(self, event_id: int) -> pd.DataFrame:
        """Restituisce gli eventi chiave (gol, cartellini, sostituzioni) di una partita.

        I file sono indicizzati da ``eventId`` e ``playId``; il metodo
        seleziona il file corretto per stagione/lega e filtra le righe per
        l'evento specificato.
        """
        season_year, league_code = self._derive_season_league_code(event_id)
        df = self._load_files_by_pattern("keyEvents_data", f"keyEvents_{season_year}_{league_code}")
        if not df.empty and "eventId" in df.columns:
            df = df[df["eventId"] == event_id]
        return df.reset_index(drop=True)

    def lineup_for_event(self, event_id: int) -> pd.DataFrame:
        """Restituisce la formazione e le sostituzioni per una partita.

        I dati includono per ogni giocatore se è titolare o subentrato,
        il ruolo, la posizione in campo e l'orario di entrata/uscita.
        """
        season_year, league_code = self._derive_season_league_code(event_id)
        df = self._load_files_by_pattern("lineup_data", f"lineup_{season_year}_{league_code}")
        if not df.empty and "eventId" in df.columns:
            df = df[df["eventId"] == event_id]
        return df.reset_index(drop=True)

    def player_stats_for_event(self, event_id: int) -> pd.DataFrame:
        """Restituisce le statistiche individuali dei giocatori per una partita.

        Le colonne includono varie metriche: minuti giocati, tiri, passaggi,
        contrasti, cartellini, ecc.  Indicizzate da ``eventId``, ``teamId``
        e ``athleteId``.  Per unire questi dati con le informazioni sui
        giocatori, è possibile eseguire un ``merge`` con il DataFrame
        ``players()`` tramite ``athleteId``.
        """
        season_year, league_code = self._derive_season_league_code(event_id)
        df = self._load_files_by_pattern("playerStats_data", f"playerStats_{season_year}_{league_code}")
        if not df.empty and "eventId" in df.columns:
            df = df[df["eventId"] == event_id]
        return df.reset_index(drop=True)

    def plays_for_event(self, event_id: int) -> pd.DataFrame:
        """Restituisce la cronologia completa delle azioni di gioco (plays) per una partita.

        Questi dati sono di dettaglio molto fine (tutte le azioni),
        indicizzati da ``eventId`` e ``playId`` e comprendono anche la
        sequenza temporale degli eventi.
        """
        season_year, league_code = self._derive_season_league_code(event_id)
        df = self._load_files_by_pattern("plays_data", f"plays_{season_year}_{league_code}")
        if not df.empty and "eventId" in df.columns:
            df = df[df["eventId"] == event_id]
        return df.reset_index(drop=True)

    # ------------------------------------------------------------------
    # Funzioni ausiliarie
    def list_events(self, league_code: Optional[str] = None, season_year: Optional[str] = None) -> pd.Series:
        """Restituisce la serie degli ``eventId`` disponibili, filtrabile per campionato e stagione.

        Parametri
        ----------
        league_code : str, optional
            Sigla del campionato (ad esempio ``"ITA_SERIEA"`` o ``"ita1"``).  Se
            fornita, seleziona solo gli eventi di quel campionato.  La
            funzione rimuove caratteri non alfanumerici e confronta in
            maiuscolo.
        season_year : str, optional
            Anno della stagione (ad esempio ``"2024"``).  Se specificato,
            filtra per stagione.
        
        Ritorna
        -------
        pandas.Series
            Serie di ``eventId`` che soddisfano i filtri.
        """
        fixtures = self.fixtures()
        mask = pd.Series([True] * len(fixtures))
        if league_code:
            code_clean = re.sub(r"[^A-Za-z0-9]", "", league_code).upper()
            # confronta con leagueId e midsizeName puliti
            league_ids = fixtures.get("leagueId", pd.Series([None] * len(fixtures))).fillna("")
            mids = fixtures.get("midsizeName", pd.Series([None] * len(fixtures))).fillna("")
            league_ids_clean = league_ids.astype(str).str.replace(r"[^A-Za-z0-9]", "", regex=True).str.upper()
            mids_clean = mids.astype(str).str.replace(r"[^A-Za-z0-9]", "", regex=True).str.upper()
            mask &= (league_ids_clean == code_clean) | (mids_clean == code_clean)
        if season_year:
            mask &= fixtures.get("season", pd.Series([None] * len(fixtures))).astype(str) == str(season_year)
        return fixtures.loc[mask, "eventId"].reset_index(drop=True)

    # PATCH — incolla in espn_data_reader.py sostituendo la funzione merge_player_stats

    def merge_player_stats(self, player_stats_df):
        """Unisce stats giocatori con anagrafiche e nomi team.
        Robusto a colonne mancanti e aggiunge posizione dal lineup se disponibile."""
        import pandas as pd

        df = player_stats_df.copy()
        if df.empty:
            return df

        # chiavi come stringhe
        for c in ("athleteId", "teamId", "eventId"):
            if c in df.columns:
                df[c] = df[c].astype(str)

        # ---- players
        players = self.players().copy()
        if "athleteId" in players.columns:
            players["athleteId"] = players["athleteId"].astype(str)

        keep_player_cols = [c for c in ["athleteId", "shortName", "displayName", "nationality"] if c in players.columns]
        players = players[keep_player_cols].rename(
            columns={k: v for k, v in {
                "shortName": "playerShortName",
                "displayName": "playerName",
                "nationality": "playerNationality",
            }.items() if k in keep_player_cols}
        )

        out = df.merge(players, on="athleteId", how="left")

        # ---- teams
        teams = self.teams().copy()
        team_id_col = "teamId" if "teamId" in teams.columns else None
        if team_id_col:
            teams[team_id_col] = teams[team_id_col].astype(str)
            keep_team_cols = [c for c in [team_id_col, "displayName", "shortName", "abbrev"] if c in teams.columns]
            teams = teams[keep_team_cols].rename(
                columns={k: v for k, v in {
                    "displayName": "teamName",
                    "shortName": "teamShortName",
                    "abbrev": "teamAbbrev",
                }.items() if k in keep_team_cols}
            )
            out = out.merge(teams, left_on="teamId", right_on=team_id_col, how="left").drop(columns=[team_id_col], errors="ignore")

        # ---- posizione dal lineup (se possibile)
        event_id = None
        if "eventId" in df.columns and df["eventId"].notna().any():
            event_id = str(df["eventId"].dropna().astype(str).iloc[0])

        if event_id:
            lineup = self.lineup_for_event(event_id)
            if not lineup.empty:
                lineup = lineup.copy()
                if "athleteId" in lineup.columns:
                    lineup["athleteId"] = lineup["athleteId"].astype(str)
                    pos_col = next((c for c in ["position", "positionName", "playerPosition",
                                                "positionFullName", "positionAbbr"] if c in lineup.columns), None)
                    if pos_col:
                        lineup = lineup[["athleteId", pos_col]].drop_duplicates()
                        out = out.merge(lineup, on="athleteId", how="left").rename(columns={pos_col: "position"})

        return out


    def to_csv(self, df: pd.DataFrame, path: Path | str) -> None:
        """Esporta un DataFrame su disco in formato CSV.

        Parametri
        ----------
        df : pandas.DataFrame
            Il DataFrame da salvare.
        path : str or Path
            Percorso di destinazione (file CSV).  Se la directory non
            esiste verrà creata.
        """
        path = Path(path)
        path.parent.mkdir(parents=True, exist_ok=True)
        df.to_csv(path, index=False)


__all__ = [
    "ESPNDataRepository",
]