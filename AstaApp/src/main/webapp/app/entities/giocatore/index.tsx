import React from 'react';
import { Route } from 'react-router';

import ErrorBoundaryRoutes from 'app/shared/error/error-boundary-routes';

import Giocatore from './giocatore';
import GiocatoreDetail from './giocatore-detail';
import GiocatoreUpdate from './giocatore-update';
import GiocatoreDeleteDialog from './giocatore-delete-dialog';

const GiocatoreRoutes = () => (
  <ErrorBoundaryRoutes>
    <Route index element={<Giocatore />} />
    <Route path="new" element={<GiocatoreUpdate />} />
    <Route path=":id">
      <Route index element={<GiocatoreDetail />} />
      <Route path="edit" element={<GiocatoreUpdate />} />
      <Route path="delete" element={<GiocatoreDeleteDialog />} />
    </Route>
  </ErrorBoundaryRoutes>
);

export default GiocatoreRoutes;
