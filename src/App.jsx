import React, { useState, useEffect } from 'react';
import { BrowserRouter as Router, Routes, Route, Navigate, Outlet } from 'react-router-dom';
import { Toaster } from '@/components/ui/toaster';
import { supabase } from '@/lib/supabaseClient';
import TopNavigation from '@/components/TopNavigation'; 
import AuthPage from '@/pages/AuthPage';
import DashboardPage from '@/pages/DashboardPage';
import CampaignsPage from '@/pages/CampaignsPage';
import AdGroupsPage from '@/pages/AdGroupsPage'; 
import KeywordsPage from '@/pages/KeywordsPage';
import LinkAmazonPage from '@/pages/LinkAmazonPage';
import AmazonCallbackPage from '@/pages/AmazonCallbackPage'; 
import OptimizationPage from '@/pages/OptimizationPage';
import { LayoutDashboard, BarChart3, Users as AdGroupsIcon, Tag, Settings as SettingsIcon, Link2 as LinkIcon, Zap, Zap as OptimizationIcon } from 'lucide-react';

const ProtectedRoute = ({ user }) => {
  if (!user) {
    return <Navigate to="/auth" replace />;
  }
  return <Outlet />;
};

function App() {
  const [user, setUser] = useState(null);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    const getSession = async () => {
      const { data: { session } } = await supabase.auth.getSession();
      setUser(session?.user ?? null);
      setLoading(false);
    };

    getSession();

    const { data: authListener } = supabase.auth.onAuthStateChange(
      (_event, session) => {
        setUser(session?.user ?? null);
      }
    );

    return () => {
      authListener?.subscription.unsubscribe();
    };
  }, []);

  const navItems = user ? [
    { href: '/', icon: <LayoutDashboard />, label: 'Dashboard' },
    { href: '/campaigns', icon: <BarChart3 />, label: 'Campaigns' },
    { href: '/ad-groups', icon: <AdGroupsIcon />, label: 'Ad Groups' }, 
    { href: '/keywords', icon: <Tag />, label: 'Keywords' },
    { href: '/optimization', icon: <OptimizationIcon />, label: 'Optimization' },
    { href: '/link-amazon', icon: <LinkIcon />, label: 'KDP Accounts' },
  ] : [];

  if (loading) {
    return (
      <div className="min-h-screen flex items-center justify-center bg-background">
        <div className="flex flex-col items-center">
          <Zap size={64} className="text-primary animate-pulse mb-4" />
          <p className="text-xl text-foreground">Loading Robotads...</p>
        </div>
      </div>
    );
  }

  return (
    <Router>
      <div className="min-h-screen flex flex-col bg-background text-foreground">
        {user && <TopNavigation navItems={navItems} user={user} />} 
        <main
          className={`flex-grow transition-all duration-300 ease-in-out ${
            user ? 'pt-16' : ''
          }`}
        >
          <div className={user ? 'p-4 sm:p-6 lg:p-8' : ''}>
            <Routes>
              <Route path="/auth" element={user ? <Navigate to="/" /> : <AuthPage />} />
              <Route path="/amazon-callback" element={<AmazonCallbackPage />} />
              <Route element={<ProtectedRoute user={user} />}>
                <Route path="/" element={<DashboardPage />} />
                <Route path="/campaigns" element={<CampaignsPage />} />
                <Route path="/ad-groups" element={<AdGroupsPage />} /> 
                <Route path="/keywords" element={<KeywordsPage />} />
                <Route path="/optimization" element={<OptimizationPage />} />
                <Route path="/link-amazon" element={<LinkAmazonPage />} />
              </Route>
              <Route path="*" element={<Navigate to={user ? "/" : "/auth"} />} />
            </Routes>
          </div>
        </main>
        <Toaster />
      </div>
    </Router>
  );
}

export default App;