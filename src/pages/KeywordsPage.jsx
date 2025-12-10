import React, { useState, useEffect, useCallback } from 'react';
import { supabase } from '@/lib/supabaseClient';
import { useToast } from '@/components/ui/use-toast';
import { Button } from '@/components/ui/button';
import { Card, CardContent, CardHeader, CardTitle, CardDescription } from "@/components/ui/card";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table";
import { Tag, AlertTriangle, DownloadCloud, Loader2, Search, ArrowUpDown, RefreshCcw } from 'lucide-react';
import { motion } from 'framer-motion';
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";
import { Input } from '@/components/ui/input';
import { useNavigate } from 'react-router-dom';

const generateSimulatedKeywords = (count = 20, adGroupId = 'sim-ag-1') => {
  const statuses = ['enabled', 'paused', 'enabled', 'archived'];
  const matchTypes = ['broad', 'phrase', 'exact', 'broad'];
  const texts = ["mystery books", "thriller novels", "best sellers", "new releases", "kindle unlimited", "sci-fi adventure", "historical fiction", "romance ebooks", "crime stories", "fantasy series"];
  let keywords = [];
  for (let i = 0; i < count; i++) {
    const spend = Math.random() * 100 + 10;
    const orders = Math.floor(Math.random() * 10 + 1);
    const sales = orders * (Math.random() * 20 + 5);
    const clicks = Math.floor(Math.random() * 80 + 10);
    const impressions = clicks * Math.floor(Math.random() * 30 + 15);

    keywords.push({
      id: `sim-kw-${i}`,
      text: texts[i % texts.length] + (matchTypes[i % matchTypes.length] === 'exact' ? ` [${i+1}]` : ` ${i+1}`),
      match_type: matchTypes[i % matchTypes.length],
      status: statuses[i % statuses.length],
      bid: Math.random() * 1.5 + 0.2,
      amazon_keyword_id: `sim-amzn-kw-${i}`,
      ad_group_id: adGroupId,
      amazon_ad_groups: { name: `Simulated Ad Group ${adGroupId.slice(-1)}` }, // Simulated ad group name
      raw_data: {
        spend: spend,
        orders: orders,
        impressions: impressions,
        clicks: clicks,
        sales: sales,
      },
      acos: sales > 0 ? spend / sales : 0,
      ctr: impressions > 0 ? clicks / impressions : 0,
      cpc: clicks > 0 ? spend / clicks : 0,
    });
  }
  return keywords;
};


const KeywordsPage = () => {
  const [keywords, setKeywords] = useState([]);
  const [linkedAccounts, setLinkedAccounts] = useState([]);
  const [selectedAccountId, setSelectedAccountId] = useState('');
  const [currentAccountStatus, setCurrentAccountStatus] = useState('');
  const [adGroups, setAdGroups] = useState([]);
  const [selectedAdGroupId, setSelectedAdGroupId] = useState(''); 

  const [loadingData, setLoadingData] = useState(false);
  const [loadingAccounts, setLoadingAccounts] = useState(true);
  const [loadingAdGroups, setLoadingAdGroups] = useState(false);
  
  const [syncingAccount, setSyncingAccount] = useState(null);
  const [searchTerm, setSearchTerm] = useState('');
  const [sortConfig, setSortConfig] = useState({ key: 'text', direction: 'ascending' });
  const { toast } = useToast();
  const [user, setUser] = useState(null);
  const navigate = useNavigate();
  const [showSimulatedData, setShowSimulatedData] = useState(false);

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
          .select('id, name, last_sync, status, client_id, amazon_profile_id')
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
            setShowSimulatedData(false);
        }
      } catch (error) {
        toast({ title: "Error fetching accounts", description: error.message, variant: "destructive" });
        setShowSimulatedData(false);
      }
    } else {
        setShowSimulatedData(false);
    }
    setLoadingAccounts(false);
  }, [toast]);

  useEffect(() => {
    fetchUserAndAccounts();
  }, [fetchUserAndAccounts]);

  const fetchAdGroupsForAccount = useCallback(async () => {
    if (!selectedAccountId || !user) {
        setAdGroups([]);
        setSelectedAdGroupId('');
        if (!user || linkedAccounts.length === 0) setShowSimulatedData(false);
        return;
    }
    setLoadingAdGroups(true);
    setShowSimulatedData(false);
    try {
      const { data, error } = await supabase
        .from('amazon_ad_groups')
        .select('id, name, amazon_ad_group_id') 
        .eq('account_id', selectedAccountId)
        .order('name', { ascending: true });
      if (error) throw error;
      setAdGroups(data || []);
      if (data && data.length > 0) {
        setSelectedAdGroupId('all'); 
      } else {
        setSelectedAdGroupId('');
        if (currentAccountStatus === 'active') setShowSimulatedData(false);
      }
    } catch (error) {
      toast({ title: "Error fetching ad groups for account", description: error.message, variant: "destructive" });
      setAdGroups([]);
      setShowSimulatedData(false);
    } finally {
      setLoadingAdGroups(false);
    }
  }, [selectedAccountId, user, toast, currentAccountStatus, linkedAccounts]);
  
  useEffect(() => {
    if (selectedAccountId && user) {
      fetchAdGroupsForAccount();
      const selectedAcc = linkedAccounts.find(acc => acc.id === selectedAccountId);
      setCurrentAccountStatus(selectedAcc?.status || '');
    } else {
        setAdGroups([]);
        setSelectedAdGroupId('');
        if (!user || linkedAccounts.length === 0) setShowSimulatedData(false);
    }
  }, [selectedAccountId, user, fetchAdGroupsForAccount, linkedAccounts]);


  const fetchKeywords = useCallback(async () => {
    if (!selectedAccountId || !user) {
        setKeywords([]);
        if (!user || linkedAccounts.length === 0) setShowSimulatedData(false);
        return;
    }
    setLoadingData(true);
    setShowSimulatedData(false);
    try {
      const currentAmazonAccount = linkedAccounts.find(acc => acc.id === selectedAccountId);
      if (!currentAmazonAccount?.amazon_profile_id) {
        setKeywords([]);
        setLoadingData(false);
        if (currentAccountStatus === 'active') setShowSimulatedData(false);
        return;
      }

      let query = supabase
        .from('amazon_keywords')
        .select('*, amazon_ad_groups (name)') 
        .eq('amazon_profile_id_text', currentAmazonAccount.amazon_profile_id);


      if (selectedAdGroupId && selectedAdGroupId !== 'all') {
        const adGroupInternalId = selectedAdGroupId; // selectedAdGroupId is already our internal UUID
        if(adGroupInternalId) {
          query = query.eq('ad_group_id', adGroupInternalId);
        } else {
           setKeywords([]); setLoadingData(false); 
           if (currentAccountStatus === 'active') setShowSimulatedData(false);
           return;
        }
      }
      
      query = query.order(sortConfig.key, { ascending: sortConfig.direction === 'ascending' });

      const { data, error } = await query;

      if (error) throw error;
      setKeywords(data || []);
      if ((data || []).length === 0 && adGroups.length > 0 && currentAccountStatus === 'active') {
        setShowSimulatedData(false);
      }
    } catch (error) {
      toast({ title: "Error fetching keywords", description: error.message, variant: "destructive" });
      setKeywords([]);
      setShowSimulatedData(false);
    } finally {
      setLoadingData(false);
    }
  }, [selectedAccountId, selectedAdGroupId, user, toast, sortConfig, linkedAccounts, adGroups, currentAccountStatus]);

  useEffect(() => {
     if (selectedAccountId && user && (selectedAdGroupId || adGroups.length === 0)) {
      fetchKeywords();
    } else if (!selectedAdGroupId && adGroups.length > 0) {
      setKeywords([]);
    } else if (!user || linkedAccounts.length === 0) {
      setShowSimulatedData(false);
    }
  }, [selectedAccountId, selectedAdGroupId, user, fetchKeywords, adGroups, linkedAccounts]);

  const handleSyncData = async (accountId) => {
    if (!user) {
        toast({ title: "Not authenticated", description: "Please login to sync data.", variant: "destructive" });
        return;
    }
    setSyncingAccount(accountId);
    setShowSimulatedData(false);
    toast({ title: "Sync Initiated", description: "Fetching latest Keyword data from Amazon. This may take a few minutes." });
    try {
      const { data: sessionData, error: sessionError } = await supabase.auth.getSession();
      if (sessionError || !sessionData.session) {
        toast({ title: "Authentication Error", description: "Your session is invalid. Please log in again.", variant: "destructive" });
        setSyncingAccount(null);
        return;
      }
      const accessToken = sessionData.session.access_token;

      const { data: funcData, error: funcError } = await supabase.functions.invoke('fetch-amazon-data', {
        headers: { Authorization: `Bearer ${accessToken}` },
        body: { account_id: accountId, action: 'sync_all_data' },
      });
      if (funcError) throw funcError;
      
      const message = funcData?.message || "Data fetching process completed.";
      const variant = funcData?.success ? "default" : "destructive";
      const needsReauth = funcData?.needsReauth || false;
      const newStatus = needsReauth ? 'reauth_required' : (funcData?.success ? 'active' : currentAccountStatus);
      
      toast({ title: funcData?.success ? "Sync Successful" : "Sync Info", description: message, variant: variant, className: funcData?.success ? "bg-green-600 text-white" : "" });
      
      fetchKeywords();
      fetchAdGroupsForAccount();

      const updatedAccounts = linkedAccounts.map(acc => 
        acc.id === accountId ? { ...acc, last_sync: new Date().toISOString(), status: newStatus } : acc
      );
      setLinkedAccounts(updatedAccounts);
      if(accountId === selectedAccountId) setCurrentAccountStatus(newStatus);
      if (!funcData?.success || needsReauth) setShowSimulatedData(false);

    } catch (error) {
      console.error("Sync Error:", error);
      const errorMessage = error.message || (error.context?.message) || 'Unknown error during sync.';
      toast({ title: "Sync Error", description: `Failed to sync keyword data: ${errorMessage}`, variant: "destructive" });
      setShowSimulatedData(false);
       if (error.context?.status === 401 || error.message.includes("reauth_required") || error.message.includes("Amazon Profile ID is missing")) {
         const updatedAccounts = linkedAccounts.map(acc => acc.id === accountId ? { ...acc, status: 'reauth_required' } : acc);
         setLinkedAccounts(updatedAccounts);
         if(accountId === selectedAccountId) setCurrentAccountStatus('reauth_required');
      }
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

  const displayedKeywords = showSimulatedData ? generateSimulatedKeywords(20, selectedAdGroupId || 'sim-ag-1') : keywords;

  const filteredKeywords = displayedKeywords.filter(kw => 
    (kw.text && kw.text.toLowerCase().includes(searchTerm.toLowerCase())) ||
    (kw.amazon_keyword_id && kw.amazon_keyword_id.toLowerCase().includes(searchTerm.toLowerCase()))
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
    setAdGroups([]);
    setKeywords([]);
    setSelectedAdGroupId('');
    setShowSimulatedData(false);
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
          <Tag size={36} className="text-purple-400" />
          <h1 className="text-3xl sm:text-4xl font-bold tracking-tight text-slate-100">Keyword Management</h1>
        </div>
        {linkedAccounts.length > 0 && selectedAccountId && currentAccountStatus !== 'reauth_required' && 
         currentAccountStatus !== 'error_no_profile' && currentAccountStatus !== 'error_no_region' && (
          <Button 
            onClick={() => handleSyncData(selectedAccountId)} 
            disabled={syncingAccount === selectedAccountId || loadingData}
            className="bg-blue-600 hover:bg-blue-700 text-white shadow-md w-full sm:w-auto"
          >
            {syncingAccount === selectedAccountId ? (
              <Loader2 size={20} className="mr-2 animate-spin" />
            ) : (
              <DownloadCloud size={20} className="mr-2" />
            )}
            Sync Keyword Data
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
              <CardTitle className="text-xl text-purple-300">Your Keywords {showSimulatedData && <span className="text-yellow-400 text-sm">(Simulated Data)</span>}</CardTitle>
              <CardDescription className="text-slate-400">
                View and manage your Amazon Advertising keywords. Last sync: {linkedAccounts.find(acc => acc.id === selectedAccountId)?.last_sync ? new Date(linkedAccounts.find(acc => acc.id === selectedAccountId)?.last_sync).toLocaleString() : 'Never'}
              </CardDescription>
            </div>
             <div className="flex flex-col sm:flex-row gap-2 w-full md:w-auto">
              {loadingAccounts ? (<Loader2 size={24} className="animate-spin text-purple-400"/>) : linkedAccounts.length > 0 ? (
                <Select onValueChange={handleAccountChange} value={selectedAccountId} disabled={loadingAdGroups || loadingData}>
                  <SelectTrigger className="w-full sm:w-[200px] bg-slate-700 border-slate-600 text-slate-100">
                    <SelectValue placeholder="Select Account" />
                  </SelectTrigger>
                  <SelectContent className="bg-slate-800 text-slate-100 border-slate-700">
                    {linkedAccounts.map(account => (
                      <SelectItem key={account.id} value={account.id}>{account.name || `Account ...${account.id.slice(-6)}`} ({account.status || 'N/A'})</SelectItem>
                    ))}
                  </SelectContent>
                </Select>
              ) : (
                 <p className="text-yellow-400 text-sm self-center">No Amazon accounts linked. <Button variant="link" className="p-0 h-auto text-purple-400" onClick={() => navigate('/link-amazon')}>Link an account</Button></p>
              )}

              {selectedAccountId && (loadingAdGroups ? <Loader2 size={24} className="animate-spin text-purple-400"/> : adGroups.length > 0 ? (
                <Select onValueChange={setSelectedAdGroupId} value={selectedAdGroupId} disabled={loadingData}>
                  <SelectTrigger className="w-full sm:w-[200px] bg-slate-700 border-slate-600 text-slate-100">
                    <SelectValue placeholder="Select Ad Group" />
                  </SelectTrigger>
                  <SelectContent className="bg-slate-800 text-slate-100 border-slate-700">
                    <SelectItem value="all">All Ad Groups</SelectItem>
                    {adGroups.map(ag => (
                      <SelectItem key={ag.id} value={ag.id}>{ag.name}</SelectItem>
                    ))}
                  </SelectContent>
                </Select>
              ) : !loadingAdGroups && !showSimulatedData && <p className="text-xs text-slate-400 self-center">No ad groups for this account.</p>)}
            </div>
          </div>
           <div className="mt-4">
            <div className="relative w-full max-w-xs">
              <Search className="absolute left-3 top-1/2 transform -translate-y-1/2 h-4 w-4 text-slate-400" />
              <Input 
                type="text"
                placeholder="Search keywords..."
                value={searchTerm}
                onChange={(e) => setSearchTerm(e.target.value)}
                className="pl-10 bg-slate-700 border-slate-600 text-slate-100 placeholder-slate-500"
                disabled={(!selectedAccountId && !showSimulatedData) || loadingData || (currentAccountStatus === 'reauth_required' || currentAccountStatus === 'error_no_profile' || currentAccountStatus === 'error_no_region')}
              />
            </div>
          </div>
        </CardHeader>
        <CardContent>
          {loadingData && selectedAccountId && !showSimulatedData && currentAccountStatus !== 'reauth_required' && currentAccountStatus !== 'error_no_profile' && currentAccountStatus !== 'error_no_region' ? (
            <div className="flex justify-center items-center min-h-[200px]">
              <Loader2 size={32} className="animate-spin text-purple-400" />
              <p className="ml-3 text-slate-300">Loading keywords...</p>
            </div>
          ) : !selectedAccountId && !showSimulatedData ? (
             <div className="text-center py-10">
                <AlertTriangle size={48} className="mx-auto text-yellow-400 mb-4 opacity-70" />
                <p className="text-slate-300 text-lg">Please select an Amazon account.</p>
                <p className="text-slate-400 text-sm mt-1">If no accounts are linked, simulated data will be shown.</p>
             </div>
          ) : (currentAccountStatus === 'reauth_required' || currentAccountStatus === 'error_no_profile' || currentAccountStatus === 'error_no_region') && !showSimulatedData ? (
             <div className="text-center py-10">
                <AlertTriangle size={48} className="mx-auto text-red-500 mb-4 opacity-70" />
                <h3 className="text-xl font-semibold text-slate-200 mb-2">
                  {currentAccountStatus === 'reauth_required' && "Account Requires Re-authentication"}
                  {currentAccountStatus === 'error_no_profile' && "Profile ID Missing"}
                  {currentAccountStatus === 'error_no_region' && "Region Invalid/Missing"}
                </h3>
                <p className="text-slate-400 mb-4">
                  {currentAccountStatus === 'reauth_required' && "The access token for this Amazon account is invalid or key information is missing. Please re-link it."}
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
          ): filteredKeywords.length === 0 && !loadingData && !showSimulatedData ? (
            <div className="text-center py-10">
              <AlertTriangle size={48} className="mx-auto text-yellow-400 mb-4 opacity-70" />
              <h3 className="text-xl font-semibold text-slate-200 mb-2">No Keywords Found</h3>
              <p className="text-slate-400">
                No keywords for the selected criteria, or data hasn't been synced.
              </p>
               <Button 
                onClick={() => handleSyncData(selectedAccountId)} 
                disabled={syncingAccount === selectedAccountId}
                className="mt-4 bg-blue-600 hover:bg-blue-700 text-white"
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
                    {['text', 'match_type', 'status', 'bid', 'spend', 'impressions', 'clicks', 'ctr', 'cpc', 'orders', 'acos'].map(key => (
                       <TableHead key={key} onClick={() => handleSort(key)} className="cursor-pointer hover:bg-slate-700/50 transition-colors text-slate-300">
                         <div className="flex items-center">
                           {key.charAt(0).toUpperCase() + key.slice(1).replace(/_/g, ' ')}
                           {getSortIcon(key)}
                         </div>
                       </TableHead>
                    ))}
                  </TableRow>
                </TableHeader>
                <TableBody>
                  {filteredKeywords.map((kw) => (
                    <TableRow key={kw.id} className="border-slate-700 hover:bg-slate-700/30">
                      <TableCell className="font-medium text-purple-300 min-w-[150px] break-all" title={kw.text}>{kw.text} <span className="text-xs text-slate-500">({kw.amazon_ad_groups?.name || 'N/A'})</span></TableCell>
                      <TableCell>{kw.match_type}</TableCell>
                      <TableCell>
                        <span className={`px-2 py-1 text-xs rounded-full ${
                          kw.status === 'enabled' || kw.status === 'active' ? 'bg-green-500/20 text-green-300' : 
                          kw.status === 'paused' ? 'bg-yellow-500/20 text-yellow-300' : 
                          'bg-red-500/20 text-red-300'
                        }`}>
                          {kw.status || 'unknown'}
                        </span>
                      </TableCell>
                      <TableCell>{formatCurrency(kw.bid)}</TableCell>
                      <TableCell>{formatCurrency(kw.spend)}</TableCell>
                      <TableCell>{formatNumber(kw.impressions)}</TableCell>
                      <TableCell>{formatNumber(kw.clicks)}</TableCell>
                      <TableCell>{formatPercentage(kw.impressions > 0 ? (kw.clicks || 0) / kw.impressions : 0)}</TableCell>
                      <TableCell>{formatCurrency(kw.clicks > 0 ? (kw.spend || 0) / kw.clicks : 0)}</TableCell>
                      <TableCell>{formatNumber(kw.orders)}</TableCell>
                      <TableCell>{formatPercentage(kw.acos)}</TableCell>
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

export default KeywordsPage;