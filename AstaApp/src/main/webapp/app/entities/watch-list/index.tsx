import React from 'react';
import { Route } from 'react-router';

import ErrorBoundaryRoutes from 'app/shared/error/error-boundary-routes';

import WatchList from './watch-list';
import WatchListDetail from './watch-list-detail';
import WatchListUpdate from './watch-list-update';
import WatchListDeleteDialog from './watch-list-delete-dialog';

const WatchListRoutes = () => (
  <ErrorBoundaryRoutes>
    <Route index element={<WatchList />} />
    <Route path="new" element={<WatchListUpdate />} />
    <Route path=":id">
      <Route index element={<WatchListDetail />} />
      <Route path="edit" element={<WatchListUpdate />} />
      <Route path="delete" element={<WatchListDeleteDialog />} />
    </Route>
  </ErrorBoundaryRoutes>
);

export default WatchListRoutes;
