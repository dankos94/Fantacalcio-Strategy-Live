import React from 'react';
import { Route } from 'react-router';

import ErrorBoundaryRoutes from 'app/shared/error/error-boundary-routes';

import Lega from './lega';
import LegaDetail from './lega-detail';
import LegaUpdate from './lega-update';
import LegaDeleteDialog from './lega-delete-dialog';

const LegaRoutes = () => (
  <ErrorBoundaryRoutes>
    <Route index element={<Lega />} />
    <Route path="new" element={<LegaUpdate />} />
    <Route path=":id">
      <Route index element={<LegaDetail />} />
      <Route path="edit" element={<LegaUpdate />} />
      <Route path="delete" element={<LegaDeleteDialog />} />
    </Route>
  </ErrorBoundaryRoutes>
);

export default LegaRoutes;
