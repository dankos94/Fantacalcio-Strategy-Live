import React from 'react';
import { Route } from 'react-router';

import ErrorBoundaryRoutes from 'app/shared/error/error-boundary-routes';

import Coach from './coach';
import CoachDetail from './coach-detail';
import CoachUpdate from './coach-update';
import CoachDeleteDialog from './coach-delete-dialog';

const CoachRoutes = () => (
  <ErrorBoundaryRoutes>
    <Route index element={<Coach />} />
    <Route path="new" element={<CoachUpdate />} />
    <Route path=":id">
      <Route index element={<CoachDetail />} />
      <Route path="edit" element={<CoachUpdate />} />
      <Route path="delete" element={<CoachDeleteDialog />} />
    </Route>
  </ErrorBoundaryRoutes>
);

export default CoachRoutes;
