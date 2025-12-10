import React, { useState, useEffect, useCallback } from 'react';
import { useNavigate } from 'react-router-dom';
import { supabase } from '@/lib/supabaseClient';
import { useToast } from '@/components/ui/use-toast';
import { Button } from '@/components/ui/button';
import { Card, CardContent, CardHeader, CardTitle, CardDescription } from "@/components/ui/card";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table";
import { BarChart3, AlertTriangle, DownloadCloud, Loader2, Search, ArrowUpDown, RefreshCcw } from 'lucide-react';
import { motion } from 'framer-motion';
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";
import { Input } from '@/components/ui/input';




const CampaignsPage = () => {
  const navigate = useNavigate();
  const [campaigns, setCampaigns] = useState([]);
  const [linkedAccounts, setLinkedAccounts] = useState([]);
  const [selectedAccountId, setSelectedAccountId] = useState('');
  const [currentAccountStatus, setCurrentAccountStatus] = useState('');
  
  const [loadingCampaigns, setLoadingCampaigns] = useState(false);
  const [loadingAccounts, setLoadingAccounts] = useState(true);
  const [syncingAccount, setSyncingAccount] = useState(null);
  const [searchTerm, setSearchTerm] = useState('');
  const [sortConfig, setSortConfig] = useState({ key: 'name', direction: 'ascending' });
  const { toast } = useToast();
  const [user, setUser] = useState(null);


  const pageVariants = {
    initial: { opacity: 0, y: 20 },
    animate: { opacity: 1, y: 0, transition: { duration: 0.5 } },
  };

  const fetchUserAndAccounts = useCallback(async () => {
    setLoadingAccounts(true);
    const { data: { user: currentUser } } = await supabase.auth.getUser();
    setUser(currentUser);
    if (currentUser) {
      try {
        const { data, error } = await supabase
          .from('amazon_accounts')
          .select('id, name, last_sync, status, client_id') 
          .eq('user_id', currentUser.id);
        if (error) throw error;
        setLinkedAccounts(data || []);
        if (data && data.length > 0) {
          const activeAccount = data.find(acc => acc.status === 'active') || data[0];
          setSelectedAccountId(activeAccount.id);
          setCurrentAccountStatus(activeAccount.status || '');
        } else {
          setSelectedAccountId('');
          setCurrentAccountStatus('');
        }
      } catch (error) {
        toast({ title: "Error fetching accounts", description: error.message, variant: "destructive" });
      }
    } else {
      // No user logged in
    }
    setLoadingAccounts(false);
  }, [toast]);

  useEffect(() => {
    fetchUserAndAccounts();
  }, [fetchUserAndAccounts]);

  const fetchCampaigns = useCallback(async () => {
    if (!selectedAccountId || !user) {
        setCampaigns([]);
        return;
    }
    setLoadingCampaigns(true);
    try {
      const { data, error } = await supabase
        .from('amazon_campaigns')
        .select('*') 
        .eq('account_id', selectedAccountId)
        .order(sortConfig.key, { ascending: sortConfig.direction === 'ascending' });

      if (error) throw error;
      setCampaigns(data || []);
    } catch (error) {
      console.error("Fetch Campaigns Error:", error);
      toast({ title: "Error fetching campaigns", description: error.message, variant: "destructive" });
      setCampaigns([]);
    } finally {
      setLoadingCampaigns(false);
    }
  }, [selectedAccountId, user, toast, sortConfig, currentAccountStatus, linkedAccounts]);

  useEffect(() => {
    if (selectedAccountId && user) {
      fetchCampaigns();
      const selectedAcc = linkedAccounts.find(acc => acc.id === selectedAccountId);
      setCurrentAccountStatus(selectedAcc?.status || '');
    } else {
      setCampaigns([]); 
    }
  }, [selectedAccountId, user, fetchCampaigns, linkedAccounts]);

  const handleSyncData = async (accountId) => {
    if (!user) {
      toast({ title: "Not authenticated", description: "Please login to sync data.", variant: "destructive" });
      return;
    }

    setSyncingAccount(accountId);
    toast({
      title: "Refreshing",
      description: "Reloading latest campaign data from the database. Server-side sync runs on the VPS.",
    });

    try {
      await fetchCampaigns();
    } catch (error) {
      console.error("Refresh Error:", error);
      const errorMessage = error.message || 'Unknown error during refresh.';
      toast({ title: "Refresh Error", description: `Failed to refresh campaign data: ${errorMessage}`, variant: "destructive" });
    } finally {
      setSyncingAccount(null);
    }
  };
  
  const handleSort = (key) => {
    let direction = 'ascending';
    if (sortConfig.key === key && sortConfig.direction === 'ascending') {
      direction = 'descending';
    }
    setSortConfig({ key, direction });
  };

  const displayedCampaigns = campaigns;

  const filteredCampaigns = displayedCampaigns.filter(campaign => 
    (campaign.name && campaign.name.toLowerCase().includes(searchTerm.toLowerCase())) ||
    (campaign.amazon_campaign_id_text && campaign.amazon_campaign_id_text.toLowerCase().includes(searchTerm.toLowerCase()))
  );

  const getSortIcon = (key) => {
    if (sortConfig.key !== key) {
      return <ArrowUpDown size={14} className="ml-2 opacity-30" />;
    }
    return sortConfig.direction === 'ascending' ? 
      <ArrowUpDown size={14} className="ml-2 text-purple-400 transform rotate-180" /> : 
      <ArrowUpDown size={14} className="ml-2 text-purple-400" />;
  };

  const formatCurrency = (value) => {
    if (value === null || typeof value === 'undefined') return 'N/A';
    const numValue = Number(value);
    if (isNaN(numValue)) return 'N/A';
    return `${numValue.toFixed(2)}`;
  };

  const formatPercentage = (value) => {
    if (value === null || typeof value === 'undefined') return 'N/A';
    const numValue = Number(value);
    if (isNaN(numValue)) return 'N/A';
    return `${(numValue * 100).toFixed(2)}%`;
  };

  const formatNumber = (value) => {
     if (value === null || typeof value === 'undefined') return 'N/A';
    const numValue = Number(value);
    if (isNaN(numValue)) return 'N/A';
    return numValue.toLocaleString();
  };

  const handleAccountChange = (value) => {
    setSelectedAccountId(value);
    const selectedAcc = linkedAccounts.find(acc => acc.id === value);
    setCurrentAccountStatus(selectedAcc?.status || '');
    setCampaigns([]); 
  };


  return (
    <motion.div 
      className="space-y-8"
      initial="initial"
      animate="animate"
      variants={pageVariants}
    >
      <div className="flex flex-col sm:flex-row items-center justify-between gap-4">
        <div className="flex items-center gap-3">
          <BarChart3 size={36} className="text-purple-400" />
          <h1 className="text-3xl sm:text-4xl font-bold tracking-tight text-slate-100">Campaign Management</h1>
        </div>
        {linkedAccounts.length > 0 && selectedAccountId && currentAccountStatus !== 'reauth_required' && 
         currentAccountStatus !== 'error_no_profile' && currentAccountStatus !== 'error_no_region' && (
          <Button 
            onClick={() => handleSyncData(selectedAccountId)} 
            disabled={syncingAccount === selectedAccountId || loadingCampaigns}
            className="bg-blue-600 hover:bg-blue-700 text-white shadow-md w-full sm:w-auto"
          >
            {syncingAccount === selectedAccountId ? (
              <Loader2 size={20} className="mr-2 animate-spin" />
            ) : (
              <DownloadCloud size={20} className="mr-2" />
            )}
            Sync Campaign Data
          </Button>
        )}
         {(currentAccountStatus === 'reauth_required' || currentAccountStatus === 'error_no_profile' || currentAccountStatus === 'error_no_region') && selectedAccountId && (
            <Button 
                onClick={() => navigate('/link-amazon', { state: { accountIdToRelink: selectedAccountId, client_id: linkedAccounts.find(acc => acc.id === selectedAccountId)?.client_id } })} 
                variant="destructive"
                className="w-full sm:w-auto bg-yellow-500 hover:bg-yellow-600 text-yellow-900 border-yellow-600 hover:border-yellow-700"
            >
                <RefreshCcw size={20} className="mr-2"/> Re-link Amazon Account
            </Button>
        )}
      </div>

      <Card className="bg-slate-800/80 border-slate-700/60 shadow-xl">
        <CardHeader>
          <div className="flex flex-col md:flex-row justify-between items-start md:items-center gap-4">
            <div>
              <CardTitle className="text-xl text-purple-300">Your Campaigns</CardTitle>
              <CardDescription className="text-slate-400">
                View and manage your Amazon Advertising campaigns. Last sync: {linkedAccounts.find(acc => acc.id === selectedAccountId)?.last_sync ? new Date(linkedAccounts.find(acc => acc.id === selectedAccountId)?.last_sync).toLocaleString() : 'Never'}
              </CardDescription>
            </div>
            {loadingAccounts ? (
                <Loader2 size={24} className="animate-spin text-purple-400"/>
            ): linkedAccounts.length > 0 ? (
              <Select onValueChange={handleAccountChange} value={selectedAccountId}>
                <SelectTrigger className="w-full md:w-[250px] bg-slate-700 border-slate-600 text-slate-100">
                  <SelectValue placeholder="Select Amazon Account" />
                </SelectTrigger>
                <SelectContent className="bg-slate-800 text-slate-100 border-slate-700">
                  {linkedAccounts.map(account => (
                    <SelectItem key={account.id} value={account.id}>{account.name || `Account ...${account.id.slice(-6)}`} ({account.status || 'unknown'})</SelectItem>
                  ))}
                </SelectContent>
              </Select>
            ) : (
                 <p className="text-yellow-400 text-sm">No Amazon accounts linked. <Button variant="link" className="p-0 h-auto text-purple-400" onClick={() => navigate('/link-amazon')}>Link an account</Button></p>
            )}
          </div>
           <div className="mt-4 flex items-center gap-2">
            <div className="relative w-full max-w-xs">
              <Search className="absolute left-3 top-1/2 transform -translate-y-1/2 h-4 w-4 text-slate-400" />
              <Input 
                type="text"
                placeholder="Search campaigns..."
                value={searchTerm}
                onChange={(e) => setSearchTerm(e.target.value)}
                className="pl-10 bg-slate-700 border-slate-600 text-slate-100 placeholder-slate-500"
                disabled={!selectedAccountId || loadingCampaigns || (currentAccountStatus === 'reauth_required' || currentAccountStatus === 'error_no_profile' || currentAccountStatus === 'error_no_region')}
              />
            </div>
          </div>
        </CardHeader>
        <CardContent>
          {loadingCampaigns && selectedAccountId && currentAccountStatus !== 'reauth_required' && currentAccountStatus !== 'error_no_profile' && currentAccountStatus !== 'error_no_region' ? (
            <div className="flex justify-center items-center min-h-[200px]">
              <Loader2 size={32} className="animate-spin text-purple-400" />
              <p className="ml-3 text-slate-300">Loading campaigns...</p>
            </div>
          ) : !selectedAccountId ? (
             <div className="text-center py-10">
                <AlertTriangle size={48} className="mx-auto text-yellow-400 mb-4 opacity-70" />
                <p className="text-slate-300 text-lg">Please select an Amazon account to view campaigns.</p>
                <p className="text-slate-400 text-sm mt-1">Please link an Amazon account to view campaigns.</p>
             </div>
          ) : (currentAccountStatus === 'reauth_required' || currentAccountStatus === 'error_no_profile' || currentAccountStatus === 'error_no_region') ? (
            <div className="text-center py-10">
                <AlertTriangle size={48} className="mx-auto text-red-500 mb-4 opacity-70" />
                <h3 className="text-xl font-semibold text-slate-200 mb-2">
                  {currentAccountStatus === 'reauth_required' && "Account Requires Re-authentication"}
                  {currentAccountStatus === 'error_no_profile' && "Profile ID Missing"}
                  {currentAccountStatus === 'error_no_region' && "Region Invalid/Missing"}
                </h3>
                <p className="text-slate-400 mb-4">
                  {currentAccountStatus === 'reauth_required' && "The access token for this Amazon account is invalid. Please re-link it."}
                  {currentAccountStatus === 'error_no_profile' && "The Amazon Profile ID is missing for this account. Please re-link to configure it."}
                  {currentAccountStatus === 'error_no_region' && "The Amazon Region is missing or invalid for this account. Please re-link to configure it."}
                </p>
                <Button 
                    onClick={() => navigate('/link-amazon', { state: { accountIdToRelink: selectedAccountId, client_id: linkedAccounts.find(acc => acc.id === selectedAccountId)?.client_id } })} 
                    variant="destructive"
                    className="bg-yellow-500 hover:bg-yellow-600 text-yellow-900 border-yellow-600 hover:border-yellow-700"
                >
                    <RefreshCcw size={18} className="mr-2"/> Re-link Account
                </Button>
            </div>
          ) : filteredCampaigns.length === 0 && !loadingCampaigns ? (
            <div className="text-center py-10">
              <AlertTriangle size={48} className="mx-auto text-yellow-400 mb-4 opacity-70" />
              <h3 className="text-xl font-semibold text-slate-200 mb-2">No Campaigns Found</h3>
              <p className="text-slate-400 mb-4">
                No campaigns found for this account. Please sync data to view campaigns.
              </p>
              <Button 
                onClick={() => handleSyncData(selectedAccountId)} 
                disabled={syncingAccount === selectedAccountId}
                className="bg-blue-600 hover:bg-blue-700 text-white"
              >
                {syncingAccount === selectedAccountId ? <Loader2 size={18} className="mr-2 animate-spin" /> : <DownloadCloud size={18} className="mr-2" />}
                Try Syncing Data
              </Button>
            </div>
          ) : (
            <div className="overflow-x-auto">
              <Table>
                <TableHeader>
                  <TableRow className="border-slate-700">
                    {['name', 'status', 'budget', 'spend', 'impressions', 'clicks', 'ctr', 'cpc', 'orders', 'acos'].map(key => (
                       <TableHead key={key} onClick={() => handleSort(key)} className="cursor-pointer hover:bg-slate-700/50 transition-colors text-slate-300">
                         <div className="flex items-center">
                           {key.charAt(0).toUpperCase() + key.slice(1).replace('_', ' ')}
                           {getSortIcon(key)}
                         </div>
                       </TableHead>
                    ))}
                  </TableRow>
                </TableHeader>
                <TableBody>
                  {filteredCampaigns.map((campaign) => (
                    <TableRow key={campaign.id} className="border-slate-700 hover:bg-slate-700/30">
                      <TableCell className="font-medium text-purple-300 min-w-[200px] break-all" title={campaign.name}>{campaign.name}</TableCell>
                      <TableCell>
                        <span className={`px-2 py-1 text-xs rounded-full ${
                          campaign.status === 'enabled' || campaign.status === 'active' ? 'bg-green-500/20 text-green-300' : 
                          campaign.status === 'paused' ? 'bg-yellow-500/20 text-yellow-300' : 
                          campaign.status === 'archived' ? 'bg-slate-500/20 text-slate-300' :
                          'bg-red-500/20 text-red-300'
                        }`}>
                          {campaign.status || 'unknown'}
                        </span>
                      </TableCell>
                      <TableCell>{formatCurrency(campaign.budget)}</TableCell>
                      <TableCell>{formatCurrency(campaign.raw_data?.spend)}</TableCell>
                      <TableCell>{formatNumber(campaign.raw_data?.impressions)}</TableCell>
                      <TableCell>{formatNumber(campaign.raw_data?.clicks)}</TableCell>
                      <TableCell>{formatPercentage(campaign.raw_data?.impressions > 0 ? (campaign.raw_data?.clicks || 0) / campaign.raw_data.impressions : 0)}</TableCell>
                      <TableCell>{formatCurrency(campaign.raw_data?.clicks > 0 ? (campaign.raw_data?.spend || 0) / campaign.raw_data.clicks : 0)}</TableCell>
                      <TableCell>{formatNumber(campaign.raw_data?.orders)}</TableCell>
                      <TableCell>{formatPercentage(campaign.raw_data?.sales > 0 ? (campaign.raw_data?.spend || 0) / campaign.raw_data.sales : 0)}</TableCell>
                    </TableRow>
                  ))}
                </TableBody>
              </Table>
            </div>
          )}
        </CardContent>
      </Card>
    </motion.div>
  );
};

export default CampaignsPage;