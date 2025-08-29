# qa_serie_a_v2.py — test qualità dati robusto per Serie A
from pathlib import Path
import random
import pandas as pd
import unicodedata as ud
from espn_data_reader import ESPNDataRepository

# --- CONFIG
DATA_DIR = Path(r"C:\Users\dnl.costantino.svi\OneDrive - SEI Consulting S.p.A\Desktop\Fanta\espn-soccer-data")
SEASON_PREFIX = None   # es. "2024" per limitare; None = tutte
SAMPLE_N = 50          # n. eventi da testare (riduci se grande)

repo = ESPNDataRepository(DATA_DIR)

def pick(df, names):
    for c in names:
        if c in df.columns:
            return c
    return None

def norm(s: str) -> str:
    s = str(s)
    s = ud.normalize("NFKD", s).encode("ascii", "ignore").decode("ascii")
    for ch in [".", "-", "_", "/", "\\", "(", ")", "[", "]", ",", ":"]:
        s = s.replace(ch, " ")
    return " ".join(s.lower().split())

# --- carica basi
fx = repo.fixtures().copy()
lg = repo.leagues().copy() if hasattr(repo, "leagues") else pd.DataFrame()

# key principali
eid_col = pick(fx, ["eventId","eventid","gameId","id","matchId"])
date_col = pick(fx, ["date","startDate","kickoff"])
season_col = pick(fx, ["seasonYear","season"])
lid_fx = pick(fx, ["leagueId","league","league_code"])
lid_lg = pick(lg, ["leagueId","league","id"])
name_lg = pick(lg, ["midsizeName","name","displayName"])

# normalizza league key in fixtures
fx["_lid_fx"] = fx[lid_fx].astype(str) if lid_fx else ""
fx["_lid_fx_norm"] = fx["_lid_fx"].map(norm) if lid_fx else ""
# normalizza league key in leagues e aggiungi nomi
if not lg.empty and lid_lg:
    lg["_lid_lg"] = lg[lid_lg].astype(str)
    lg["_lid_lg_norm"] = lg["_lid_lg"].map(norm)
    if name_lg:
        lg["_lname_norm"] = lg[name_lg].astype(str).map(norm)
    fx = fx.merge(
        lg[[lid_lg, name_lg]] if name_lg else lg[[lid_lg]],
        left_on=lid_fx, right_on=lid_lg, how="left"
    )
    if name_lg:
        fx["_lname_norm"] = fx[name_lg].astype(str).map(norm)
else:
    fx["_lname_norm"] = ""

# --- heuristic di filtro Serie A (robusto)
candidates = pd.Series(False, index=fx.index)
# 1) leagueId classico
if lid_fx:
    candidates |= fx["_lid_fx_norm"].isin(["ita 1","italy 1","italia 1"])
# 2) nome contiene "serie a" + italia
candidates |= fx["_lname_norm"].str.contains("serie a", na=False)
# 3) fallback: fixture.league/name se presente
name_fx = pick(fx, ["league","leagueName","competition"])
if name_fx:
    fx["_lname_fx_norm"] = fx[name_fx].astype(str).map(norm)
    candidates |= fx["_lname_fx_norm"].str.contains("serie a", na=False)

fx_ita = fx[candidates].copy()

# filtra per stagione se richiesto
if SEASON_PREFIX and not fx_ita.empty and season_col:
    fx_ita = fx_ita[fx_ita[season_col].astype(str).str.startswith(str(SEASON_PREFIX))]

# --- diagnostica leghe disponibili se filtro vuoto
if fx_ita.empty:
    print("ATTENZIONE: nessun evento Serie A trovato con le euristiche.")
    # suggerisci possibili chiavi presenti
    sample_leagues = (
        fx[[col for col in [lid_fx, name_lg, name_fx] if col]]
        .drop_duplicates()
        .head(25)
    )
    print("\nEsempi di valori lega presenti (prime 25 righe distinte):")
    print(sample_leagues.to_string(index=False))
    # esci in modo pulito
    print("\nNessun test eseguito.")
    raise SystemExit(0)

# --- campione eventi
fx_ita = fx_ita.dropna(subset=[eid_col]).drop_duplicates(subset=[eid_col])
events = fx_ita[eid_col].astype(str).tolist()
random.seed(0)
sample_eids = random.sample(events, k=min(SAMPLE_N, len(events)))

# --- probe coperture
cols_cov = ["eventId","has_commentary","has_keyEvents","has_lineup","has_playerStats"]
res = []
def has_rows(df): return (df is not None) and (not df.empty)
for eid in sample_eids:
    try:
        c = repo.commentary_for_event(eid)
        k = repo.key_events_for_event(eid)
        l = repo.lineup_for_event(eid)
        p = repo.player_stats_for_event(eid)
        res.append([eid, has_rows(c), has_rows(k), has_rows(l), has_rows(p)])
    except Exception:
        res.append([eid, False, False, False, False])
dfq = pd.DataFrame(res, columns=cols_cov)

# --- metriche
total = len(dfq)
def rate(s): return f"{(s.mean() if total else 0):.2%}"
print(f"Serie A — eventi totali (filtrati): {len(events)}  |  campione testato: {total}")
print("Coperture (quota eventi con ≥1 riga):")
print(f"  - commentary_cov: {rate(dfq['has_commentary'])}")
print(f"  - keyEvents_cov:  {rate(dfq['has_keyEvents'])}")
print(f"  - lineup_cov:     {rate(dfq['has_lineup'])}")
print(f"  - playerStats_cov:{rate(dfq['has_playerStats'])}")
print(f"  - all_present_cov:{rate(dfq[['has_commentary','has_keyEvents','has_lineup','has_playerStats']].all(axis=1))}")

dup_rate = fx_ita.duplicated(subset=[eid_col]).mean() if not fx_ita.empty else 0
print(f"Duplicati eventId (Serie A): {dup_rate:.2%}")

if date_col:
    null_rate_date = fx_ita[date_col].isna().mean()
    print(f"Null rate campo data '{date_col}': {null_rate_date:.2%}")

# --- salva eventi problematici se presenti
if total:
    bad = dfq[~dfq[["has_commentary","has_keyEvents","has_lineup","has_playerStats"]].any(axis=1)]
    out_csv = DATA_DIR / "qa_serie_a_missing.csv"
    bad.to_csv(out_csv, index=False)
    print(f"Eventi senza alcun dato dettagliato nel campione: {len(bad)}  -> file: {out_csv}")
else:
    print("Nessun evento nel campione: skip export file.")
