import React from 'react';
import { Route } from 'react-router';

import ErrorBoundaryRoutes from 'app/shared/error/error-boundary-routes';

import Stagione from './stagione';
import Lega from './lega';
import Squadra from './squadra';
import Coach from './coach';
import Giocatore from './giocatore';
import Roster from './roster';
import WatchList from './watch-list';
/* jhipster-needle-add-route-import - JHipster will add routes here */

export default () => {
  return (
    <div>
      <ErrorBoundaryRoutes>
        {/* prettier-ignore */}
        <Route path="stagione/*" element={<Stagione />} />
        <Route path="lega/*" element={<Lega />} />
        <Route path="squadra/*" element={<Squadra />} />
        <Route path="coach/*" element={<Coach />} />
        <Route path="giocatore/*" element={<Giocatore />} />
        <Route path="roster/*" element={<Roster />} />
        <Route path="watch-list/*" element={<WatchList />} />
        {/* jhipster-needle-add-route-path - JHipster will add routes here */}
      </ErrorBoundaryRoutes>
    </div>
  );
};
