import React from 'react';
import { Route } from 'react-router';

import ErrorBoundaryRoutes from 'app/shared/error/error-boundary-routes';

import Stagione from './stagione';
import StagioneDetail from './stagione-detail';
import StagioneUpdate from './stagione-update';
import StagioneDeleteDialog from './stagione-delete-dialog';

const StagioneRoutes = () => (
  <ErrorBoundaryRoutes>
    <Route index element={<Stagione />} />
    <Route path="new" element={<StagioneUpdate />} />
    <Route path=":id">
      <Route index element={<StagioneDetail />} />
      <Route path="edit" element={<StagioneUpdate />} />
      <Route path="delete" element={<StagioneDeleteDialog />} />
    </Route>
  </ErrorBoundaryRoutes>
);

export default StagioneRoutes;
