import React from 'react';
import { Route } from 'react-router';

import ErrorBoundaryRoutes from 'app/shared/error/error-boundary-routes';

import Roster from './roster';
import RosterDetail from './roster-detail';
import RosterUpdate from './roster-update';
import RosterDeleteDialog from './roster-delete-dialog';

const RosterRoutes = () => (
  <ErrorBoundaryRoutes>
    <Route index element={<Roster />} />
    <Route path="new" element={<RosterUpdate />} />
    <Route path=":id">
      <Route index element={<RosterDetail />} />
      <Route path="edit" element={<RosterUpdate />} />
      <Route path="delete" element={<RosterDeleteDialog />} />
    </Route>
  </ErrorBoundaryRoutes>
);

export default RosterRoutes;
