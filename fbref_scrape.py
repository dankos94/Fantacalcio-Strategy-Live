# fbref_serie_a_standard_stats_selenium.py
# Requisiti:
#   pip install selenium webdriver-manager beautifulsoup4 pandas lxml
#
# Funzioni:
# - Naviga alla pagina Serie A su FBref.
# - Estrae i link delle squadre esclusivamente dalla tabella "Serie A Table".
# - Per ogni squadra, estrae i link dei giocatori dalla tabella "Standard Stats".
# - Per ogni giocatore, trova la tabella id="stats_standard_dom_lg" (anche se in commento) e salva CSV.
# - Rate limit: <10 richieste/minuto (sleep di 7s tra navigazioni).

import os
import re
import time
from urllib.parse import urljoin
from pathlib import Path

import pandas as pd
from bs4 import BeautifulSoup, Comment

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager

BASE = "https://fbref.com"
SERIE_A_URL = "https://fbref.com/en/comps/11/Serie-A-Stats"
OUT_DIR = Path("serie_a_standard_stats")
NAV_DELAY_SEC = 7.0  # ~8-9 richieste/min controllate

# -------------------- Utility --------------------

def throttle(last_nav_ts: float) -> float:
    wait = NAV_DELAY_SEC - (time.time() - last_nav_ts)
    if wait > 0:
        time.sleep(wait)
    return time.time()

def abs_url(href: str) -> str:
    return urljoin(BASE, href)

def sanitize_filename(s: str) -> str:
    s = re.sub(r'[\\/:*?"<>|]+', "_", s)
    s = re.sub(r"\s+", "_", s).strip("_")
    return s[:180]

def make_driver(headless: bool = True) -> webdriver.Chrome:
    opts = Options()
    if headless:
        opts.add_argument("--headless=new")
    opts.add_argument("--start-maximized")
    opts.add_argument("--disable-blink-features=AutomationControlled")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-gpu")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument(
        "--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
    )
    drv = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=opts)
    drv.set_page_load_timeout(60)
    return drv

def dismiss_cookies(driver: webdriver.Chrome):
    # Tenta vari selettori/testi noti
    candidates = [
        (By.XPATH, "//button[contains(translate(., 'ACEPTILLOW', 'aceptillow'), 'accept')]"),
        (By.XPATH, "//button[contains(., 'Accept All')]"),
        (By.XPATH, "//button[contains(., 'I Accept')]"),
        (By.CSS_SELECTOR, "button[aria-label*='Accept']"),
    ]
    for by, sel in candidates:
        try:
            btn = WebDriverWait(driver, 3).until(EC.element_to_be_clickable((by, sel)))
            btn.click()
            time.sleep(0.5)
            return
        except Exception:
            pass

def get_soup(driver: webdriver.Chrome) -> BeautifulSoup:
    html = driver.page_source
    return BeautifulSoup(html, "lxml")
    
def estrai_ruolo_primario(soup: BeautifulSoup) -> str:
    """
    Ritorna uno tra 'GK', 'DF', 'MF', 'FW' leggendo il blocco meta del giocatore.
    Cerca prima nel DOM, poi (fallback) nei commenti HTML.
    """
    def _parse_from_p(p_tag) -> str | None:
        # Testo normalizzato del <p> che contiene "Position:"
        txt = " ".join(p_tag.stripped_strings)
        # Isola la porzione successiva a "Position:" e prima di "Footed:" (se presente)
        txt = re.sub(r'^.*?\bPosition:\s*', '', txt, flags=re.I)
        txt = re.split(r'\bFooted:\b', txt, maxsplit=1, flags=re.I)[0]
        m = re.search(r'\b(GK|DF|MF|FW)\b', txt)
        return m.group(1) if m else None

    # 1) DOM diretto: <div id="info"> ... <p><strong>Position:</strong> ...</p>
    for strong in soup.select('#info p > strong'):
        if strong.get_text(strip=True).lower().startswith('position:'):
            code = _parse_from_p(strong.parent)
            if code:
                return code

    # 2) Fallback DOM generico (senza #info)
    strong = soup.find('strong', string=lambda s: s and s.strip().lower().startswith('position:'))
    if strong:
        code = _parse_from_p(strong.parent)
        if code:
            return code

    # 3) Fallback nei commenti HTML (FBref spesso nasconde blocchi dentro <!-- ... -->)
    for c in soup.find_all(string=lambda x: isinstance(x, Comment) and 'Position:' in x):
        inner = BeautifulSoup(c, 'lxml')
        strong = inner.find('strong', string=lambda s: s and s.strip().lower().startswith('position:'))
        if strong:
            code = _parse_from_p(strong.parent)
            if code:
                return code

    return ""
    
    
def find_table_in_dom_or_comments(soup: BeautifulSoup, table_id: str):
    # 1) DOM diretto
    t = soup.find("table", id=table_id)
    if t:
        return t
    # 2) Nei commenti HTML
    for c in soup.find_all(string=lambda x: isinstance(x, Comment) and table_id in x):
        inner = BeautifulSoup(c, "lxml")
        t = inner.find("table", id=table_id)
        if t:
            return t
    return None

def find_first_table_starting_with(soup: BeautifulSoup, id_prefix: str):
    # 1) DOM diretto
    t = soup.find("table", id=lambda x: x and x.startswith(id_prefix))
    if t:
        return t
    # 2) Nei commenti
    for c in soup.find_all(string=lambda x: isinstance(x, Comment) and id_prefix in x):
        inner = BeautifulSoup(c, "lxml")
        t = inner.find("table", id=lambda x: x and x.startswith(id_prefix))
        if t:
            return t
    return None

# -------------------- Scraping Steps --------------------

def get_serie_a_team_links(driver: webdriver.Chrome) -> list[tuple[str, str]]:
    # Carica pagina Serie A e limita estrazione alla tabella "Serie A Table"
    driver.get(SERIE_A_URL)
    dismiss_cookies(driver)
    soup = get_soup(driver)
    caption = soup.find("caption", string="Serie A Table")
    if not caption:
        raise RuntimeError("Tabella 'Serie A Table' non trovata.")
    table = caption.find_parent("table")
    if not table:
        raise RuntimeError("Tabella classifica non trovata.")

    teams = []
    for td in table.select('tbody tr td[data-stat="team"]'):
        a = td.find("a", href=True)
        if not a:
            continue
        name = a.get_text(strip=True)
        href = abs_url(a["href"])
        # Filtra link attesi alle squadre (pattern FBref)
        if "/en/squads/" in href and href.endswith("-Stats"):
            teams.append((name, href))

    # Dedup mantenendo ordine
    seen, ordered = set(), []
    for name, url in teams:
        if url not in seen:
            seen.add(url)
            ordered.append((name, url))
    return ordered

def get_team_players(driver: webdriver.Chrome, team_url: str) -> list[tuple[str, str]]:
    driver.get(abs_url(team_url))
    soup = get_soup(driver)
    # Tabella "Standard Stats" della squadra (id che inizia con stats_standard)
    table = find_first_table_starting_with(soup, "stats_standard")
    if not table:
        return []
    players = []
    for row in table.select("tbody tr"):
        th = row.find(["th", "td"], {"data-stat": "player"})
        if not th:
            continue
        a = th.find("a", href=True)
        if not a:
            continue
        pname = a.get_text(strip=True)
        phref = abs_url(a["href"])
        if "/en/players/" in phref:
            players.append((pname, phref))
    return players

def save_player_standard_domestic_csv(driver: webdriver.Chrome, player_name: str, player_url: str, team_name: str) -> tuple[bool, str]:
    driver.get(abs_url(player_url))
    soup = get_soup(driver)

    # Tabella id="stats_standard_dom_lg" (anche in commento)
    table = find_table_in_dom_or_comments(soup, "stats_standard_dom_lg")
    if not table:
        return False, "stats_standard_dom_lg_non_trovata"

    # parse con pandas
    try:
        df_list = pd.read_html(str(table))
        if not df_list:
            return False, "read_html_vuoto"
        df = df_list[0]
    except Exception as e:
        return False, f"read_html_error:{e}"

    # opzionale: aggiungi contesto
    df.insert(0, "Player", player_name)
    df.insert(1, "Team", team_name)

    team_dir = OUT_DIR / sanitize_filename(team_name)
    team_dir.mkdir(parents=True, exist_ok=True)
    player_role = estrai_ruolo_primario(soup)
    fname = team_dir / f"{sanitize_filename(player_name)}_{player_role}.csv"
    try:
        df.to_csv(fname, index=False)
    except Exception as e:
        return False, f"csv_error:{e}"
    return True, str(fname)

# -------------------- Main --------------------

def main():
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    driver = make_driver(headless=True)
    last_nav = 0.0
    try:
        # Squadre dalla tabella Serie A
        last_nav = throttle(last_nav)
        teams = get_serie_a_team_links(driver)
        if not teams:
            raise RuntimeError("Nessuna squadra trovata nella tabella Serie A.")

        for t_idx, (team_name, team_url) in enumerate(teams, start=1):
            print(f"[{t_idx}/{len(teams)}] Squadra: {team_name}")
            last_nav = throttle(last_nav)
            players = get_team_players(driver, team_url)
            if not players:
                print(f"  Nessun giocatore trovato in 'Standard Stats' per {team_name}")
                continue

            for p_idx, (player_name, player_url) in enumerate(players, start=1):
                last_nav = throttle(last_nav)
                ok, info = save_player_standard_domestic_csv(driver, player_name, player_url, team_name)
                if ok:
                    print(f"  ({p_idx:02d}/{len(players)}) {player_name} -> {info}")
                else:
                    print(f"  ({p_idx:02d}/{len(players)}) {player_name} SKIP: {info}")

    finally:
        try:
            driver.quit()
        except Exception:
            pass

if __name__ == "__main__":
    main()
