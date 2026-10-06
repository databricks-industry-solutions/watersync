import { createBrowserRouter, RouterProvider } from 'react-router';
import { Layout } from './components/Layout';
import { OverviewPage } from './components/OverviewPage';
import { ConfigPage } from './components/ConfigPage';
import { WatermarksPage } from './components/WatermarksPage';
import { JobsPage } from './components/JobsPage';

const router = createBrowserRouter([
  {
    element: <Layout />,
    children: [
      { path: '/', element: <OverviewPage /> },
      { path: '/config', element: <ConfigPage /> },
      { path: '/watermarks', element: <WatermarksPage /> },
      { path: '/jobs', element: <JobsPage /> },
    ],
  },
]);
export default function App() {
  return <RouterProvider router={router} />;
}
