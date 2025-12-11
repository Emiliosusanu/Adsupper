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
import { OPTIMIZATION_SERVER_URL } from '@/lib/config';

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
  const [selectedIds, setSelectedIds] = useState(new Set());
  const [editedBids, setEditedBids] = useState({});
  const [editedStatuses, setEditedStatuses] = useState({});
  const [batchBid, setBatchBid] = useState('');
  const [batchStatus, setBatchStatus] = useState('');
  const [applying, setApplying] = useState(false);

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
    toast({
      title: "Refreshing",
      description: "Reloading latest keyword data from the database. Server-side sync runs on the VPS.",
    });
    try {
      await fetchKeywords();
      await fetchAdGroupsForAccount();
    } catch (error) {
      console.error("Refresh Error:", error);
      const errorMessage = error.message || 'Unknown error during refresh.';
      toast({ title: "Refresh Error", description: `Failed to refresh keyword data: ${errorMessage}`, variant: "destructive" });
      setShowSimulatedData(false);
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

  const toggleSelected = (id, checked) => {
    setSelectedIds((prev) => {
      const next = new Set(prev);
      if (checked) next.add(id); else next.delete(id);
      return next;
    });
  };

  const handleBidChange = (id, value) => {
    setEditedBids((prev) => ({ ...prev, [id]: value }));
  };

  const handleStatusChange = (id, value) => {
    setEditedStatuses((prev) => ({ ...prev, [id]: value }));
  };

  const callUpdate = async (type, items) => {
    const res = await fetch(`${OPTIMIZATION_SERVER_URL}/amazon/update`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ accountId: selectedAccountId, type, items }),
    });
    const json = await res.json().catch(() => ({}));
    if (!res.ok || json.status !== 'success') throw new Error(json.message || `HTTP ${res.status}`);
  };

  const applyRow = async (row) => {
    setApplying(true);
    try {
      const ops = [];
      if (editedBids[row.id] != null && editedBids[row.id] !== '') {
        const num = Number(editedBids[row.id]);
        if (Number.isNaN(num)) throw new Error('Invalid bid');
        ops.push(callUpdate('keyword', [{ amazonId: row.amazon_keyword_id, value: num }]));
      }
      if (editedStatuses[row.id]) {
        ops.push(callUpdate('keyword_status', [{ amazonId: row.amazon_keyword_id, value: String(editedStatuses[row.id]) }]));
      }
      if (ops.length === 0) return;
      await Promise.all(ops);
      toast({ title: 'Applied', description: 'Keyword updated', variant: 'default' });
      await fetchKeywords();
    } catch (e) {
      toast({ title: 'Apply failed', description: String(e.message || e), variant: 'destructive' });
    } finally {
      setApplying(false);
    }
  };

  const applyBatchBid = async () => {
    const ids = Array.from(selectedIds);
    if (ids.length === 0) { toast({ title: 'No selection', description: 'Select rows first', variant: 'destructive' }); return; }
    const num = Number(batchBid);
    if (Number.isNaN(num)) { toast({ title: 'Invalid bid', description: 'Enter a number', variant: 'destructive' }); return; }
    setApplying(true);
    try {
      const items = keywords.filter((r) => ids.includes(r.id)).map((r) => ({ amazonId: r.amazon_keyword_id, value: num }));
      await callUpdate('keyword', items);
      toast({ title: 'Applied', description: `Updated ${items.length} keyword(s)`, variant: 'default' });
      await fetchKeywords();
    } catch (e) {
      toast({ title: 'Apply failed', description: String(e.message || e), variant: 'destructive' });
    } finally {
      setApplying(false);
    }
  };

  const applyBatchStatus = async () => {
    const ids = Array.from(selectedIds);
    if (ids.length === 0) { toast({ title: 'No selection', description: 'Select rows first', variant: 'destructive' }); return; }
    if (!batchStatus) { toast({ title: 'Missing status', description: 'Choose a status', variant: 'destructive' }); return; }
    setApplying(true);
    try {
      const items = keywords.filter((r) => ids.includes(r.id)).map((r) => ({ amazonId: r.amazon_keyword_id, value: batchStatus }));
      await callUpdate('keyword_status', items);
      toast({ title: 'Applied', description: `Updated ${items.length} keyword(s)`, variant: 'default' });
      await fetchKeywords();
    } catch (e) {
      toast({ title: 'Apply failed', description: String(e.message || e), variant: 'destructive' });
    } finally {
      setApplying(false);
    }
  };

  const exportCsv = () => {
    const rows = filteredKeywords;
    const headers = ['text','match_type','status','bid','spend','impressions','clicks','ctr','cpc','orders','acos','ad_group_name','amazon_keyword_id'];
    const lines = [headers.join(',')];
    for (const r of rows) {
      const cells = [
        r.text ?? '',
        r.match_type ?? '',
        r.status ?? '',
        r.bid ?? '',
        r.spend ?? '',
        r.impressions ?? '',
        r.clicks ?? '',
        (r.impressions > 0 ? (r.clicks || 0) / r.impressions : 0),
        (r.clicks > 0 ? (r.spend || 0) / r.clicks : 0),
        r.orders ?? '',
        r.acos ?? '',
        r.amazon_ad_groups?.name ?? '',
        r.amazon_keyword_id ?? '',
      ];
      lines.push(cells.map((c) => ("\"" + String(c).replaceAll('"','""') + "\"" )).join(','));
    }
    const blob = new Blob([lines.join('\n')], { type: 'text/csv;charset=utf-8;' });
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = 'keywords.csv';
    a.click();
    URL.revokeObjectURL(url);
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
              <Input type="number" step="0.01" placeholder="Batch bid" value={batchBid} onChange={(e) => setBatchBid(e.target.value)} className="w-full sm:w-[140px] bg-slate-700 border-slate-600 text-slate-100" />
              <Button onClick={applyBatchBid} disabled={applying || selectedIds.size === 0} className="bg-emerald-600 hover:bg-emerald-700 text-white">Apply Bids</Button>
              <Select onValueChange={setBatchStatus} value={batchStatus}>
                <SelectTrigger className="w-full sm:w-[160px] bg-slate-700 border-slate-600 text-slate-100"><SelectValue placeholder="Set status" /></SelectTrigger>
                <SelectContent className="bg-slate-800 text-slate-100 border-slate-700">
                  <SelectItem value="enabled">enabled</SelectItem>
                  <SelectItem value="paused">paused</SelectItem>
                  <SelectItem value="archived">archived</SelectItem>
                </SelectContent>
              </Select>
              <Button onClick={applyBatchStatus} disabled={applying || !batchStatus || selectedIds.size === 0} className="bg-amber-600 hover:bg-amber-700 text-white">Apply Status</Button>
              <Button onClick={exportCsv} variant="secondary" className="bg-slate-700 text-slate-100">Export CSV</Button>
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
                    <TableHead className="w-[36px]">
                      <input type="checkbox" onChange={(e) => {
                        const all = new Set(e.target.checked ? filteredKeywords.map(r => r.id) : []);
                        setSelectedIds(all);
                      }} checked={filteredKeywords.length > 0 && filteredKeywords.every(r => selectedIds.has(r.id))} />
                    </TableHead>
                    {['text', 'match_type', 'status', 'bid', 'spend', 'impressions', 'clicks', 'ctr', 'cpc', 'orders', 'acos'].map(key => (
                       <TableHead key={key} onClick={() => handleSort(key)} className="cursor-pointer hover:bg-slate-700/50 transition-colors text-slate-300">
                         <div className="flex items-center">
                           {key.charAt(0).toUpperCase() + key.slice(1).replace(/_/g, ' ')}
                           {getSortIcon(key)}
                         </div>
                       </TableHead>
                    ))}
                    <TableHead>New bid</TableHead>
                    <TableHead>Set status</TableHead>
                    <TableHead>Actions</TableHead>
                  </TableRow>
                </TableHeader>
                <TableBody>
                  {filteredKeywords.map((kw) => (
                    <TableRow key={kw.id} className="border-slate-700 hover:bg-slate-700/30">
                      <TableCell><input type="checkbox" checked={selectedIds.has(kw.id)} onChange={(e) => toggleSelected(kw.id, e.target.checked)} /></TableCell>
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
                      <TableCell>
                        <Input type="number" step="0.01" value={editedBids[kw.id] ?? kw.bid ?? ''} onChange={(e) => handleBidChange(kw.id, e.target.value)} className="w-28 bg-slate-700 border-slate-600 text-slate-100" />
                      </TableCell>
                      <TableCell>
                        <Select onValueChange={(v) => handleStatusChange(kw.id, v)} value={editedStatuses[kw.id] ?? ''}>
                          <SelectTrigger className="w-[140px] bg-slate-700 border-slate-600 text-slate-100"><SelectValue placeholder="choose" /></SelectTrigger>
                          <SelectContent className="bg-slate-800 text-slate-100 border-slate-700">
                            <SelectItem value="enabled">enabled</SelectItem>
                            <SelectItem value="paused">paused</SelectItem>
                            <SelectItem value="archived">archived</SelectItem>
                          </SelectContent>
                        </Select>
                      </TableCell>
                      <TableCell>
                        <Button onClick={() => applyRow(kw)} disabled={applying} className="bg-emerald-600 hover:bg-emerald-700 text-white">Apply</Button>
                      </TableCell>
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