# player_val.py
# Analisi CSV giocatori (FBref export con header multi-livello), gestione riga di riepilogo ",{n} Seasons,"
# Output: player_values.csv, player_values_columns.csv, player_values_errors.csv

from pathlib import Path
import pandas as pd
import numpy as np
import re, math

# ===== Config =====
ROOT_DIR = Path("serie_a_standard_stats")   # serie_a_standard_stats/{team}/{player}.csv
OUT_CSV  = Path("player_values.csv")
DOC_CSV  = Path("player_values_columns.csv")
ERR_CSV  = Path("player_values_errors.csv")

# ===== Utility =====
def season_to_year_end(s: str):
    m = re.match(r'(\d{4})-(\d{4})', str(s))
    return int(m.group(2)) if m else None

def to_num(x):
    try:
        if isinstance(x, str):
            x = x.replace(",", "")
        return float(x)
    except Exception:
        return np.nan

def per90_from(row: pd.Series, name: str, total_val: float, nineties: float):
    val = to_num(row.get(f"{name}_per90")) if f"{name}_per90" in row.index else np.nan
    if not (isinstance(val, float) and not math.isnan(val)):
        if (not math.isnan(total_val)) and (not math.isnan(nineties)) and nineties > 0:
            val = total_val / nineties
    return val

def read_player_csv_clean(path: Path) -> pd.DataFrame:
    """
    Appiattisce header FBref:
    - Se la prima riga contiene etichette 'vere', le usa come header.
    - Rinomina blocco 'Per 90 Minutes' con suffisso *_per90.
    """
    df = pd.read_csv(path)
    if df.empty:
        return df

    row0 = df.iloc[0]
    if any(col.startswith("Unnamed") for col in df.columns) or any("." in c for c in df.columns):
        new_cols = []
        for col in df.columns:
            label = row0[col]
            group = col.split(".")[0]
            if isinstance(label, str) and label.strip():
                if group.startswith("Per 90 Minutes"):
                    new_cols.append(f"{label}_per90")
                else:
                    new_cols.append(label)
            else:
                new_cols.append(col)
        df.columns = new_cols
        df = df.iloc[1:].reset_index(drop=True)

    # Normalizza colonne note
    for key in ["Player", "Team", "Squad", "Comp", "Country"]:
        if key in df.columns:
            df[key] = df[key].ffill().bfill()

    if "Season" in df.columns:
        df["Season"] = df["Season"].astype(str).str.strip()

    return df

def find_summary_row(df: pd.DataFrame):
    """
    Trova la PRIMA riga di riepilogo che contiene la stringa ',{n} Seasons,' in QUALSIASI cella.
    Restituisce (row_series, index) oppure (None, None).
    """
    pat = re.compile(r',\s*\d+\s+Seasons,', re.I)
    for idx, row in df.iterrows():
        # concatena celle non NaN
        txt = ",".join([str(x) for x in row.values if not (isinstance(x, float) and math.isnan(x))])
        if pat.search(txt):
            return row, idx
    return None, None

def career_from_summary(row: pd.Series):
    """Estrae totali carriera dalla riga summary."""
    def g(c): return to_num(row.get(c)) if c in row.index else np.nan
    mp = g("MP"); starts = g("Starts"); minutes = g("Min"); n90 = g("90s")
    gls = g("Gls"); ast = g("Ast"); ga = (0 if math.isnan(gls) else gls) + (0 if math.isnan(ast) else ast)
    xg = g("xG"); xa = g("xAG"); xgxa = g("xG+xAG"); npxg = g("npxG"); npxg_xa = g("npxG+xAG")
    g90  = per90_from(row, "Gls", gls, n90)
    a90  = per90_from(row, "Ast", ast, n90)
    ga90 = per90_from(row, "G+A", ga,  n90)
    xg90 = per90_from(row, "xG", xg, n90)
    xa90 = per90_from(row, "xAG", xa, n90)
    xgxa90 = per90_from(row, "xG+xAG", (0 if math.isnan(xg) else xg)+(0 if math.isnan(xa) else xa), n90)

    return {
        "Career_MP": mp, "Career_Starts": starts, "Career_Min": minutes, "Career_90s": n90,
        "Career_Gls": gls, "Career_Ast": ast, "Career_G+A": ga,
        "Career_xG": xg, "Career_xAG": xa, "Career_xG+xAG": xgxa, "Career_npxG": npxg, "Career_npxG+xAG": npxg_xa,
        "Career_Gls/90": g90, "Career_Ast/90": a90, "Career_G+A/90": ga90,
        "Career_xG/90": xg90, "Career_xAG/90": xa90, "Career_xG+xAG/90": xgxa90
    }

def evaluate_player_from_df(df_all: pd.DataFrame, filepath: Path):
    # Trova riga di riepilogo e seziona stagioni vs summary
    summary_row, summary_idx = find_summary_row(df_all)
    career = career_from_summary(summary_row) if summary_row is not None else {}

    # Stagioni: escludi la sezione di summary e righe non-stagione
    if summary_idx is not None:
        df_seasons = df_all.iloc[:summary_idx].copy()
    else:
        df_seasons = df_all.copy()

    if "Season" not in df_seasons.columns:
        return None, f"missing Season | {filepath.name}"

    df_seasons["season_end"] = df_seasons["Season"].apply(season_to_year_end)
    df_seasons = df_seasons[df_seasons["season_end"].notnull()].sort_values("season_end")
    if df_seasons.empty:
        return None, f"no season rows | {filepath.name}"

    player = str(df_all["Player"].dropna().iloc[0]) if "Player" in df_all.columns and df_all["Player"].notna().any() else filepath.stem
    team_fallback = str(df_all["Team"].dropna().iloc[0]) if "Team" in df_all.columns and df_all["Team"].notna().any() else None

    last = df_seasons.iloc[-1]
    prev = df_seasons.iloc[-2] if len(df_seasons) >= 2 else None

    def g(col, row=last): return to_num(row.get(col)) if col in row.index else np.nan

    season   = last.get("Season")
    age      = last.get("Age")
    squad    = last.get("Squad")
    comp     = last.get("Comp")
    country  = last.get("Country")
    mp       = g("MP")
    starts   = g("Starts")
    minutes  = g("Min")
    nineties = g("90s")

    gls = g("Gls"); ast = g("Ast")
    ga  = (0 if math.isnan(gls) else gls) + (0 if math.isnan(ast) else ast)
    gpk = g("G-PK"); pk = g("PK"); pkatt = g("PKatt")
    crdy = g("CrdY"); crdr = g("CrdR")

    xg   = g("xG");   xa   = g("xAG")
    npxg = g("npxG"); xgxa = g("xG+xAG")
    npxg_xa = g("npxG+xAG")

    prgP = g("PrgP"); prgC = g("PrgC"); prgR = g("PrgR")

    gls90  = per90_from(last, "Gls", gls, nineties)
    ast90  = per90_from(last, "Ast", ast, nineties)
    ga90   = per90_from(last, "G+A", ga, nineties)
    xg90   = per90_from(last, "xG", xg, nineties)
    xa90   = per90_from(last, "xAG", xa, nineties)
    xgxa90 = per90_from(last, "xG+xAG", (0 if math.isnan(xg) else xg) + (0 if math.isnan(xa) else xa), nineties)

    xg_diff = (0 if math.isnan(gls) else gls) - (0 if math.isnan(xg) else xg)
    xa_diff = (0 if math.isnan(ast) else ast) - (0 if math.isnan(xa) else xa)

    # Trend ultime 3 stagioni: GA/90
    recent = df_seasons.tail(3).copy()
    def row_ga90(row):
        n90 = to_num(row.get("90s"))
        ga90_ = to_num(row.get("G+A_per90")) if "G+A_per90" in row.index else np.nan
        if math.isnan(ga90_):
            g_ = to_num(row.get("Gls")); a_ = to_num(row.get("Ast"))
            if not math.isnan(g_) and not math.isnan(a_) and not math.isnan(n90) and n90 > 0:
                return (g_ + a_) / n90
        return ga90_
    recent["ga90"] = recent.apply(row_ga90, axis=1)
    trend = "→"
    vals = recent["ga90"].dropna().tolist()
    if len(vals) >= 2:
        if vals[-1] > vals[-2] * 1.10: trend = "↗"
        elif vals[-1] < vals[-2] * 0.90: trend = "↘"

    # Cambio lega
    new_league = "No"
    if prev is not None:
        prev_country = str(prev.get("Country")) if "Country" in prev.index else None
        prev_comp    = str(prev.get("Comp"))    if "Comp"    in prev.index else None
        if (country and prev_country and country != prev_country) or (comp and prev_comp and comp != prev_comp):
            new_league = "Yes"

    starts_pct = None
    if not math.isnan(starts) and not math.isnan(mp) and mp > 0:
        starts_pct = round(starts / mp, 3)

    rec = {
        "Player": player,
        "Team": team_fallback or squad,
        "Season": season,
        "Age": age,
        "Squad": squad,
        "Comp": comp,
        "Country": country,
        "MP": mp,
        "Starts": starts,
        "Starts_pct": starts_pct,
        "Min": minutes,
        "90s": nineties,
        "Gls": gls,
        "Ast": ast,
        "G+A": ga,
        "G-PK": gpk,
        "PK": pk,
        "PKatt": pkatt,
        "CrdY": crdy,
        "CrdR": crdr,
        "xG": xg,
        "xAG": xa,
        "npxG": npxg,
        "xG+xAG": xgxa,
        "npxG+xAG": npxg_xa,
        "PrgP": prgP,
        "PrgC": prgC,
        "PrgR": prgR,
        "Gls/90": gls90,
        "Ast/90": ast90,
        "G+A/90": ga90,
        "xG/90": xg90,
        "xAG/90": xa90,
        "xG+xAG/90": xgxa90,
        "xG_diff": xg_diff,
        "xA_diff": xa_diff,
        "Trend": trend,
        "NewLeague": new_league,
        "SourceFile": str(filepath),
    }

    # Aggiungi carriera (dal riepilogo) se presente
    rec.update({k: career.get(k, np.nan) for k in [
        "Career_MP","Career_Starts","Career_Min","Career_90s",
        "Career_Gls","Career_Ast","Career_G+A",
        "Career_xG","Career_xAG","Career_xG+xAG","Career_npxG","Career_npxG+xAG",
        "Career_Gls/90","Career_Ast/90","Career_G+A/90","Career_xG/90","Career_xAG/90","Career_xG+xAG/90"
    ]})

    return rec, None

# ===== Main =====
def main():
    rows, errors = [], []

    files = sorted(ROOT_DIR.rglob("*.csv"))
    if not files:
        print(f"Nessun CSV trovato in {ROOT_DIR.resolve()}")
        return

    for f in files:
        try:
            df = read_player_csv_clean(f)
            rec, err = evaluate_player_from_df(df, f)
            if err:
                errors.append({"file": str(f), "error": err})
            elif rec:
                rows.append(rec)
        except Exception as e:
            errors.append({"file": str(f), "error": repr(e)})

    if rows:
        out_df = pd.DataFrame(rows)
        sort_cols = [c for c in ["G+A/90", "xG+xAG/90", "Gls/90", "Ast/90"] if c in out_df.columns]
        out_df = out_df.sort_values(sort_cols, ascending=[False]*len(sort_cols), na_position="last")
        out_df.to_csv(OUT_CSV, index=False, encoding="utf-8-sig")
        print(f"OK -> {OUT_CSV.resolve()} ({len(out_df)} righe)")
    else:
        print("Nessun record valido generato.")

    if errors:
        pd.DataFrame(errors).to_csv(ERR_CSV, index=False, encoding="utf-8-sig")
        print(f"Log errori -> {ERR_CSV.resolve()} ({len(errors)} voci)")

    # Dizionario colonne
    feature_doc = [
        ("Player","Nome giocatore"),
        ("Team","Team corrente (fallback)"),
        ("Season","Ultima stagione valida (YYYY-YYYY)"),
        ("Age","Età nella stagione"),
        ("Squad","Squadra della stagione"),
        ("Comp","Competizione/lega"),
        ("Country","Paese della competizione"),
        ("MP","Presenze"),("Starts","Titolare"),("Starts_pct","Quota titolarità su MP"),
        ("Min","Minuti"),("90s","Numero di 90'"),
        ("Gls","Gol"),("Ast","Assist"),("G+A","Gol+Assist"),
        ("G-PK","Gol non su rigore"),("PK","Rigori segnati"),("PKatt","Rigori tentati"),
        ("CrdY","Ammonizioni"),("CrdR","Espulsioni"),
        ("xG","Expected Goals"),("xAG","Expected Assists"),
        ("npxG","Non-penalty xG"),("xG+xAG","xG + xAG"),("npxG+xAG","npxG + xAG"),
        ("PrgP","Progressive Passes"),("PrgC","Progressive Carries"),("PrgR","Progressive Passes Received"),
        ("Gls/90","Gol per 90'"),("Ast/90","Assist per 90'"),("G+A/90","Gol+Assist per 90'"),
        ("xG/90","xG per 90'"),("xAG/90","xAG per 90'"),("xG+xAG/90","xG+xAG per 90'"),
        ("xG_diff","Gol - xG"),("xA_diff","Assist - xAG"),
        ("Trend","Tendenza GA/90 (↗/→/↘)"),("NewLeague","Cambio lega/competizione"),
        # Carriera da riga riepilogo
        ("Career_MP","Carriera: presenze totali"),("Career_Starts","Carriera: titolare"),
        ("Career_Min","Carriera: minuti"),("Career_90s","Carriera: 90'"),
        ("Career_Gls","Carriera: gol"),("Career_Ast","Carriera: assist"),("Career_G+A","Carriera: gol+assist"),
        ("Career_xG","Carriera: xG"),("Career_xAG","Carriera: xAG"),("Career_xG+xAG","Carriera: xG+xAG"),
        ("Career_npxG","Carriera: npxG"),("Career_npxG+xAG","Carriera: npxG+xAG"),
        ("Career_Gls/90","Carriera: gol/90"),("Career_Ast/90","Carriera: assist/90"),
        ("Career_G+A/90","Carriera: gol+assist/90"),
        ("Career_xG/90","Carriera: xG/90"),("Career_xAG/90","Carriera: xAG/90"),("Career_xG+xAG/90","Carriera: xG+xAG/90"),
        ("SourceFile","Percorso file sorgente"),
    ]
    pd.DataFrame(feature_doc, columns=["column", "meaning"]).to_csv(DOC_CSV, index=False, encoding="utf-8-sig")

if __name__ == "__main__":
    main()
