import React from 'react';
// eslint-disable-line

import MenuItem from 'app/shared/layout/menus/menu-item'; // eslint-disable-line

const EntitiesMenu = () => {
  return (
    <>
      {/* prettier-ignore */}
      <MenuItem icon="asterisk" to="/stagione">
        Stagione
      </MenuItem>
      <MenuItem icon="asterisk" to="/lega">
        Lega
      </MenuItem>
      <MenuItem icon="asterisk" to="/squadra">
        Squadra
      </MenuItem>
      <MenuItem icon="asterisk" to="/coach">
        Coach
      </MenuItem>
      <MenuItem icon="asterisk" to="/giocatore">
        Giocatore
      </MenuItem>
      <MenuItem icon="asterisk" to="/roster">
        Roster
      </MenuItem>
      <MenuItem icon="asterisk" to="/watch-list">
        Watch List
      </MenuItem>
      {/* jhipster-needle-add-entity-to-menu - JHipster will add entities to the menu here */}
    </>
  );
};

export default EntitiesMenu;
