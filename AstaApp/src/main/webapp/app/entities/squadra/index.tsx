import React from 'react';
import { Route } from 'react-router';

import ErrorBoundaryRoutes from 'app/shared/error/error-boundary-routes';

import Squadra from './squadra';
import SquadraDetail from './squadra-detail';
import SquadraUpdate from './squadra-update';
import SquadraDeleteDialog from './squadra-delete-dialog';

const SquadraRoutes = () => (
  <ErrorBoundaryRoutes>
    <Route index element={<Squadra />} />
    <Route path="new" element={<SquadraUpdate />} />
    <Route path=":id">
      <Route index element={<SquadraDetail />} />
      <Route path="edit" element={<SquadraUpdate />} />
      <Route path="delete" element={<SquadraDeleteDialog />} />
    </Route>
  </ErrorBoundaryRoutes>
);

export default SquadraRoutes;
