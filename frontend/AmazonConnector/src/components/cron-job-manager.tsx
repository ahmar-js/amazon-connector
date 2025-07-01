import React, { useState, useEffect } from 'react';
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from './ui/card';
import { Button } from './ui/button';
import { Badge } from './ui/badge';
import { Alert, AlertDescription } from './ui/alert';
import { Tabs, TabsContent, TabsList, TabsTrigger } from './ui/tabs';
import { Switch } from './ui/switch';
import { Input } from './ui/input';
import { Label } from './ui/label';
import { Textarea } from './ui/textarea';
import { 
  CronJobAPI,
  type CronJobStatusResponse,
  type CronJobConfiguration,
  type CronJobLog, 
  type CronJobStats,
  formatDuration,
  formatCronExpression, 
  getStatusColor, 
  getStatusIcon 
} from '../lib/cron-api';
import { 
  Play, 
  Pause, 
  Settings, 
  Clock, 
  Activity, 
  AlertCircle, 
  CheckCircle, 
  XCircle, 
  RefreshCw,
  Server,
  Database,
  Calendar,
  BarChart3,
  History,
  Loader2,
  Save,
  Eye,
  Download
} from 'lucide-react';

interface CronJobManagerProps {
  className?: string;
}

export const CronJobManager: React.FC<CronJobManagerProps> = ({ className = '' }) => {
  const [jobStatus, setJobStatus] = useState<CronJobStatusResponse | null>(null);
  const [jobLogs, setJobLogs] = useState<CronJobLog[]>([]);
  const [jobStats, setJobStats] = useState<CronJobStats | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [activeTab, setActiveTab] = useState('overview');
  const [refreshInterval, setRefreshInterval] = useState<NodeJS.Timeout | null>(null);
  const [lastRefresh, setLastRefresh] = useState<Date>(new Date());

  // Configuration editing states
  const [editingConfig, setEditingConfig] = useState<{ [key: string]: boolean }>({});
  const [configChanges, setConfigChanges] = useState<{ [key: string]: Partial<CronJobConfiguration> }>({});

  // Manual trigger states
  const [triggering, setTriggering] = useState<{ [key: string]: boolean }>({});

  useEffect(() => {
    loadData();
    
    // Set up auto-refresh every 30 seconds
    const interval = setInterval(() => {
      loadData();
    }, 30000);
    
    setRefreshInterval(interval);
    
    return () => {
      if (interval) clearInterval(interval);
    };
  }, []);

  const loadData = async () => {
    try {
      setError(null);
      
      // Load all data in parallel
      const [statusData, logsData, statsData] = await Promise.all([
        CronJobAPI.getJobStatus(),
        CronJobAPI.getJobLogs(undefined, 20, 0),
        CronJobAPI.getJobStats(30)
      ]);
      
      setJobStatus(statusData);
      setJobLogs(logsData.logs);
      setJobStats(statsData);
      setLastRefresh(new Date());
      
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Failed to load cron job data');
    } finally {
      setLoading(false);
    }
  };

  const handleTriggerJob = async (jobType: string) => {
    try {
      setTriggering(prev => ({ ...prev, [jobType]: true }));
      
      const result = await CronJobAPI.triggerJob(jobType);
      
      // Show success message and refresh data
      alert(`${jobType} job triggered successfully! Task ID: ${result.task_id}`);
      loadData();
      
    } catch (err) {
      alert(`Failed to trigger ${jobType} job: ${err instanceof Error ? err.message : 'Unknown error'}`);
    } finally {
      setTriggering(prev => ({ ...prev, [jobType]: false }));
    }
  };

  const handleConfigChange = (jobType: string, field: string, value: any) => {
    setConfigChanges(prev => ({
      ...prev,
      [jobType]: {
        ...prev[jobType],
        [field]: value
      }
    }));
  };

  const handleSaveConfig = async (jobType: string) => {
    try {
      const changes = configChanges[jobType];
      if (!changes) return;

      await CronJobAPI.updateJobConfiguration(jobType, changes);
      
      // Clear editing state and refresh data
      setEditingConfig(prev => ({ ...prev, [jobType]: false }));
      setConfigChanges(prev => ({ ...prev, [jobType]: {} }));
      loadData();
      
      alert(`Configuration saved for ${jobType} job`);
      
    } catch (err) {
      alert(`Failed to save configuration: ${err instanceof Error ? err.message : 'Unknown error'}`);
    }
  };

  const handleCancelEdit = (jobType: string) => {
    setEditingConfig(prev => ({ ...prev, [jobType]: false }));
    setConfigChanges(prev => ({ ...prev, [jobType]: {} }));
  };

  const getJobConfig = (jobType: string): CronJobConfiguration | null => {
    if (!jobStatus) return null;
    return jobStatus.jobs[jobType as keyof typeof jobStatus.jobs]?.configuration || null;
  };

  const getJobStatusData = (jobType: string) => {
    if (!jobStatus) return null;
    return jobStatus.jobs[jobType as keyof typeof jobStatus.jobs]?.status || null;
  };

  const getCurrentConfig = (jobType: string): CronJobConfiguration | null => {
    const originalConfig = getJobConfig(jobType);
    if (!originalConfig) return null;
    
    const changes = configChanges[jobType] || {};
    return { ...originalConfig, ...changes };
  };

  if (loading) {
    return (
      <div className={`p-6 ${className}`}>
        <div className="flex items-center justify-center h-64">
          <Loader2 className="h-8 w-8 animate-spin" />
          <span className="ml-2 text-lg">Loading cron job data...</span>
        </div>
      </div>
    );
  }

  if (error) {
    return (
      <div className={`p-6 ${className}`}>
        <Alert variant="destructive">
          <AlertCircle className="h-4 w-4" />
          <AlertDescription>{error}</AlertDescription>
        </Alert>
        <Button onClick={loadData} className="mt-4">
          <RefreshCw className="h-4 w-4 mr-2" />
          Retry
        </Button>
      </div>
    );
  }

  return (
    <div className={`p-6 space-y-6 ${className}`}>
      {/* Header */}
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-3xl font-bold">Cron Job Manager</h1>
          <p className="text-muted-foreground">
            Manage automated data fetching and syncing jobs
          </p>
        </div>
        <div className="flex items-center space-x-4">
          <Badge variant="outline" className="text-sm">
            Last updated: {lastRefresh.toLocaleTimeString()}
          </Badge>
          <Button onClick={loadData} variant="outline" size="sm">
            <RefreshCw className="h-4 w-4 mr-2" />
            Refresh
          </Button>
        </div>
      </div>

      {/* System Status */}
      {jobStatus && (
        <Card>
          <CardHeader>
            <CardTitle className="flex items-center">
              <Server className="h-5 w-5 mr-2" />
              System Status
            </CardTitle>
          </CardHeader>
          <CardContent>
            <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
              <div className="flex items-center space-x-2">
                <div className={`w-3 h-3 rounded-full ${jobStatus.system_status.celery_status.active ? 'bg-green-500' : 'bg-red-500'}`} />
                <span className="font-medium">Celery Workers</span>
                <Badge variant={jobStatus.system_status.celery_status.active ? 'default' : 'destructive'}>
                  {jobStatus.system_status.celery_status.active ? 'Active' : 'Inactive'}
                </Badge>
              </div>
              <div className="flex items-center space-x-2">
                <div className={`w-3 h-3 rounded-full ${jobStatus.system_status.redis_status.active ? 'bg-green-500' : 'bg-red-500'}`} />
                <span className="font-medium">Redis</span>
                <Badge variant={jobStatus.system_status.redis_status.active ? 'default' : 'destructive'}>
                  {jobStatus.system_status.redis_status.active ? 'Connected' : 'Disconnected'}
                </Badge>
              </div>
              <div className="flex items-center space-x-2">
                <div className={`w-3 h-3 rounded-full ${jobStatus.system_status.any_job_running ? 'bg-blue-500' : 'bg-gray-500'}`} />
                <span className="font-medium">Jobs Running</span>
                <Badge variant={jobStatus.system_status.any_job_running ? 'default' : 'secondary'}>
                  {jobStatus.system_status.any_job_running ? 'Yes' : 'No'}
                </Badge>
              </div>
            </div>
          </CardContent>
        </Card>
      )}

      {/* Main Content Tabs */}
      <Tabs value={activeTab} onValueChange={setActiveTab}>
        <TabsList className="grid w-full grid-cols-4">
          <TabsTrigger value="overview">
            <Activity className="h-4 w-4 mr-2" />
            Overview
          </TabsTrigger>
          <TabsTrigger value="configuration">
            <Settings className="h-4 w-4 mr-2" />
            Configuration
          </TabsTrigger>
          <TabsTrigger value="logs">
            <History className="h-4 w-4 mr-2" />
            Execution Logs
          </TabsTrigger>
          <TabsTrigger value="analytics">
            <BarChart3 className="h-4 w-4 mr-2" />
            Analytics
          </TabsTrigger>
        </TabsList>

        {/* Overview Tab */}
        <TabsContent value="overview" className="space-y-6">
          {jobStatus && (
            <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
              {/* Fetching Job Card */}
              <Card>
                <CardHeader>
                  <CardTitle className="flex items-center justify-between">
                    <div className="flex items-center">
                      <Download className="h-5 w-5 mr-2" />
                      Fetching Job
                    </div>
                    <Badge variant={getJobStatusData('fetching')?.status === 'running' ? 'default' : 'secondary'}>
                      {getStatusIcon(getJobStatusData('fetching')?.status || 'idle')} {getJobStatusData('fetching')?.status || 'idle'}
                    </Badge>
                  </CardTitle>
                  <CardDescription>
                    Fetches order/item data from Amazon SP-API
                  </CardDescription>
                </CardHeader>
                <CardContent className="space-y-4">
                  <div className="grid grid-cols-2 gap-4 text-sm">
                    <div>
                      <Label className="text-muted-foreground">Last Run</Label>
                      <p className="font-medium">
                        {getJobStatusData('fetching')?.last_run 
                          ? new Date(getJobStatusData('fetching')!.last_run!).toLocaleString()
                          : 'Never'
                        }
                      </p>
                    </div>
                    <div>
                      <Label className="text-muted-foreground">Duration</Label>
                      <p className="font-medium">
                        {formatDuration(getJobStatusData('fetching')?.last_duration || null)}
                      </p>
                    </div>
                    <div>
                      <Label className="text-muted-foreground">Schedule</Label>
                      <p className="font-medium">
                        {formatCronExpression(getJobConfig('fetching')?.cron_expression || '')}
                      </p>
                    </div>
                    <div>
                      <Label className="text-muted-foreground">Enabled</Label>
                      <p className="font-medium">
                        {getJobConfig('fetching')?.enabled ? 'Yes' : 'No'}
                      </p>
                    </div>
                  </div>
                  
                  {getJobStatusData('fetching')?.error_message && (
                    <Alert variant="destructive">
                      <XCircle className="h-4 w-4" />
                      <AlertDescription>
                        {getJobStatusData('fetching')?.error_message}
                      </AlertDescription>
                    </Alert>
                  )}
                  
                  <Button 
                    onClick={() => handleTriggerJob('fetching')} 
                    disabled={triggering.fetching || jobStatus.system_status.any_job_running}
                    className="w-full"
                  >
                    {triggering.fetching ? (
                      <Loader2 className="h-4 w-4 mr-2 animate-spin" />
                    ) : (
                      <Play className="h-4 w-4 mr-2" />
                    )}
                    Trigger Now
                  </Button>
                </CardContent>
              </Card>

              {/* Syncing Job Card */}
              <Card>
                <CardHeader>
                  <CardTitle className="flex items-center justify-between">
                    <div className="flex items-center">
                      <Database className="h-5 w-5 mr-2" />
                      Syncing Job
                    </div>
                    <Badge variant={getJobStatusData('syncing')?.status === 'running' ? 'default' : 'secondary'}>
                      {getStatusIcon(getJobStatusData('syncing')?.status || 'idle')} {getJobStatusData('syncing')?.status || 'idle'}
                    </Badge>
                  </CardTitle>
                  <CardDescription>
                    Syncs fetched data into internal database
                  </CardDescription>
                </CardHeader>
                <CardContent className="space-y-4">
                  <div className="grid grid-cols-2 gap-4 text-sm">
                    <div>
                      <Label className="text-muted-foreground">Last Run</Label>
                      <p className="font-medium">
                        {getJobStatusData('syncing')?.last_run 
                          ? new Date(getJobStatusData('syncing')!.last_run!).toLocaleString()
                          : 'Never'
                        }
                      </p>
                    </div>
                    <div>
                      <Label className="text-muted-foreground">Duration</Label>
                      <p className="font-medium">
                        {formatDuration(getJobStatusData('syncing')?.last_duration || null)}
                      </p>
                    </div>
                    <div>
                      <Label className="text-muted-foreground">Schedule</Label>
                      <p className="font-medium">
                        {formatCronExpression(getJobConfig('syncing')?.cron_expression || '')}
                      </p>
                    </div>
                    <div>
                      <Label className="text-muted-foreground">Enabled</Label>
                      <p className="font-medium">
                        {getJobConfig('syncing')?.enabled ? 'Yes' : 'No'}
                      </p>
                    </div>
                  </div>
                  
                  {getJobStatusData('syncing')?.error_message && (
                    <Alert variant="destructive">
                      <XCircle className="h-4 w-4" />
                      <AlertDescription>
                        {getJobStatusData('syncing')?.error_message}
                      </AlertDescription>
                    </Alert>
                  )}
                  
                  <Button 
                    onClick={() => handleTriggerJob('syncing')} 
                    disabled={triggering.syncing || jobStatus.system_status.any_job_running}
                    className="w-full"
                  >
                    {triggering.syncing ? (
                      <Loader2 className="h-4 w-4 mr-2 animate-spin" />
                    ) : (
                      <Play className="h-4 w-4 mr-2" />
                    )}
                    Trigger Now
                  </Button>
                </CardContent>
              </Card>
            </div>
          )}
        </TabsContent>

        {/* Configuration Tab */}
        <TabsContent value="configuration" className="space-y-6">
          {jobStatus && (
            <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
              {/* Fetching Job Configuration */}
              <Card>
                <CardHeader>
                  <CardTitle className="flex items-center justify-between">
                    <div className="flex items-center">
                      <Download className="h-5 w-5 mr-2" />
                      Fetching Job Configuration
                    </div>
                    <div className="flex space-x-2">
                      {editingConfig.fetching ? (
                        <>
                          <Button size="sm" onClick={() => handleSaveConfig('fetching')}>
                            <Save className="h-4 w-4 mr-1" />
                            Save
                          </Button>
                          <Button size="sm" variant="outline" onClick={() => handleCancelEdit('fetching')}>
                            Cancel
                          </Button>
                        </>
                      ) : (
                        <Button size="sm" variant="outline" onClick={() => setEditingConfig(prev => ({ ...prev, fetching: true }))}>
                          <Settings className="h-4 w-4 mr-1" />
                          Edit
                        </Button>
                      )}
                    </div>
                  </CardTitle>
                </CardHeader>
                <CardContent className="space-y-4">
                  {(() => {
                    const config = getCurrentConfig('fetching');
                    if (!config) return null;
                    
                    return (
                      <>
                        <div className="flex items-center justify-between">
                          <Label htmlFor="fetching-enabled">Enabled</Label>
                          <Switch
                            id="fetching-enabled"
                            checked={config.enabled}
                            disabled={!editingConfig.fetching}
                            onCheckedChange={(checked) => handleConfigChange('fetching', 'enabled', checked)}
                          />
                        </div>
                        
                        <div className="space-y-2">
                          <Label htmlFor="fetching-cron">Cron Expression</Label>
                          <Input
                            id="fetching-cron"
                            value={config.cron_expression}
                            disabled={!editingConfig.fetching}
                            onChange={(e) => handleConfigChange('fetching', 'cron_expression', e.target.value)}
                            placeholder="0 0 */15 * *"
                          />
                          <p className="text-sm text-muted-foreground">
                            Current: {formatCronExpression(config.cron_expression)}
                          </p>
                        </div>
                        
                        <div className="space-y-2">
                          <Label htmlFor="fetching-days">Date Range (Days)</Label>
                          <Input
                            id="fetching-days"
                            type="number"
                            value={config.date_range_days}
                            disabled={!editingConfig.fetching}
                            onChange={(e) => handleConfigChange('fetching', 'date_range_days', parseInt(e.target.value))}
                            min="1"
                            max="90"
                          />
                        </div>
                        
                        <div className="space-y-2">
                          <Label htmlFor="fetching-description">Description</Label>
                          <Textarea
                            id="fetching-description"
                            value={config.description}
                            disabled={!editingConfig.fetching}
                            onChange={(e) => handleConfigChange('fetching', 'description', e.target.value)}
                            rows={3}
                          />
                        </div>
                      </>
                    );
                  })()}
                </CardContent>
              </Card>

              {/* Syncing Job Configuration */}
              <Card>
                <CardHeader>
                  <CardTitle className="flex items-center justify-between">
                    <div className="flex items-center">
                      <Database className="h-5 w-5 mr-2" />
                      Syncing Job Configuration
                    </div>
                    <div className="flex space-x-2">
                      {editingConfig.syncing ? (
                        <>
                          <Button size="sm" onClick={() => handleSaveConfig('syncing')}>
                            <Save className="h-4 w-4 mr-1" />
                            Save
                          </Button>
                          <Button size="sm" variant="outline" onClick={() => handleCancelEdit('syncing')}>
                            Cancel
                          </Button>
                        </>
                      ) : (
                        <Button size="sm" variant="outline" onClick={() => setEditingConfig(prev => ({ ...prev, syncing: true }))}>
                          <Settings className="h-4 w-4 mr-1" />
                          Edit
                        </Button>
                      )}
                    </div>
                  </CardTitle>
                </CardHeader>
                <CardContent className="space-y-4">
                  {(() => {
                    const config = getCurrentConfig('syncing');
                    if (!config) return null;
                    
                    return (
                      <>
                        <div className="flex items-center justify-between">
                          <Label htmlFor="syncing-enabled">Enabled</Label>
                          <Switch
                            id="syncing-enabled"
                            checked={config.enabled}
                            disabled={!editingConfig.syncing}
                            onCheckedChange={(checked) => handleConfigChange('syncing', 'enabled', checked)}
                          />
                        </div>
                        
                        <div className="space-y-2">
                          <Label htmlFor="syncing-cron">Cron Expression</Label>
                          <Input
                            id="syncing-cron"
                            value={config.cron_expression}
                            disabled={!editingConfig.syncing}
                            onChange={(e) => handleConfigChange('syncing', 'cron_expression', e.target.value)}
                            placeholder="0 0 */7 * *"
                          />
                          <p className="text-sm text-muted-foreground">
                            Current: {formatCronExpression(config.cron_expression)}
                          </p>
                        </div>
                        
                        <div className="space-y-2">
                          <Label htmlFor="syncing-days">Sync Days Back</Label>
                          <Input
                            id="syncing-days"
                            type="number"
                            value={config.sync_days_back}
                            disabled={!editingConfig.syncing}
                            onChange={(e) => handleConfigChange('syncing', 'sync_days_back', parseInt(e.target.value))}
                            min="1"
                            max="365"
                          />
                        </div>
                        
                        <div className="space-y-2">
                          <Label htmlFor="syncing-description">Description</Label>
                          <Textarea
                            id="syncing-description"
                            value={config.description}
                            disabled={!editingConfig.syncing}
                            onChange={(e) => handleConfigChange('syncing', 'description', e.target.value)}
                            rows={3}
                          />
                        </div>
                      </>
                    );
                  })()}
                </CardContent>
              </Card>
            </div>
          )}
        </TabsContent>

        {/* Logs Tab */}
        <TabsContent value="logs" className="space-y-6">
          <Card>
            <CardHeader>
              <CardTitle className="flex items-center">
                <History className="h-5 w-5 mr-2" />
                Recent Execution Logs
              </CardTitle>
              <CardDescription>
                Latest 20 job executions across all job types
              </CardDescription>
            </CardHeader>
            <CardContent>
              <div className="space-y-4">
                {jobLogs.map((log, index) => (
                  <div key={index} className="border rounded-lg p-4 space-y-2">
                    <div className="flex items-center justify-between">
                      <div className="flex items-center space-x-2">
                        <Badge variant="outline" className="capitalize">
                          {log.job_type}
                        </Badge>
                        <Badge variant={log.status === 'completed' ? 'default' : log.status === 'failed' ? 'destructive' : 'secondary'}>
                          {getStatusIcon(log.status)} {log.status}
                        </Badge>
                      </div>
                      <span className="text-sm text-muted-foreground">
                        {new Date(log.started_at).toLocaleString()}
                      </span>
                    </div>
                    
                    <div className="grid grid-cols-2 md:grid-cols-4 gap-4 text-sm">
                      <div>
                        <Label className="text-muted-foreground">Duration</Label>
                        <p className="font-medium">{formatDuration(log.duration)}</p>
                      </div>
                      <div>
                        <Label className="text-muted-foreground">Records</Label>
                        <p className="font-medium">{log.records_processed || 0}</p>
                      </div>
                      <div>
                        <Label className="text-muted-foreground">Task ID</Label>
                        <p className="font-mono text-xs">{log.task_id.substring(0, 8)}...</p>
                      </div>
                      <div>
                        <Label className="text-muted-foreground">Completed</Label>
                        <p className="font-medium">
                          {log.completed_at ? new Date(log.completed_at).toLocaleTimeString() : 'N/A'}
                        </p>
                      </div>
                    </div>
                    
                    {log.error_message && (
                      <Alert variant="destructive">
                        <XCircle className="h-4 w-4" />
                        <AlertDescription>{log.error_message}</AlertDescription>
                      </Alert>
                    )}
                    
                    {log.details && Object.keys(log.details).length > 0 && (
                      <details className="mt-2">
                        <summary className="cursor-pointer text-sm font-medium">View Details</summary>
                        <pre className="mt-2 p-2 bg-muted rounded text-xs overflow-x-auto">
                          {JSON.stringify(log.details, null, 2)}
                        </pre>
                      </details>
                    )}
                  </div>
                ))}
                
                {jobLogs.length === 0 && (
                  <div className="text-center py-8 text-muted-foreground">
                    No execution logs found
                  </div>
                )}
              </div>
            </CardContent>
          </Card>
        </TabsContent>

        {/* Analytics Tab */}
        <TabsContent value="analytics" className="space-y-6">
          {jobStats && (
            <>
              {/* Summary Cards */}
              <div className="grid grid-cols-1 md:grid-cols-4 gap-4">
                <Card>
                  <CardContent className="p-6">
                    <div className="flex items-center justify-between">
                      <div>
                        <p className="text-sm font-medium text-muted-foreground">Total Executions</p>
                        <p className="text-2xl font-bold">{jobStats.summary.total_executions}</p>
                      </div>
                      <Activity className="h-8 w-8 text-muted-foreground" />
                    </div>
                  </CardContent>
                </Card>
                
                <Card>
                  <CardContent className="p-6">
                    <div className="flex items-center justify-between">
                      <div>
                        <p className="text-sm font-medium text-muted-foreground">Successful</p>
                        <p className="text-2xl font-bold text-green-600">{jobStats.summary.successful_executions}</p>
                      </div>
                      <CheckCircle className="h-8 w-8 text-green-600" />
                    </div>
                  </CardContent>
                </Card>
                
                <Card>
                  <CardContent className="p-6">
                    <div className="flex items-center justify-between">
                      <div>
                        <p className="text-sm font-medium text-muted-foreground">Failed</p>
                        <p className="text-2xl font-bold text-red-600">{jobStats.summary.failed_executions}</p>
                      </div>
                      <XCircle className="h-8 w-8 text-red-600" />
                    </div>
                  </CardContent>
                </Card>
                
                <Card>
                  <CardContent className="p-6">
                    <div className="flex items-center justify-between">
                      <div>
                        <p className="text-sm font-medium text-muted-foreground">Avg Duration</p>
                        <p className="text-2xl font-bold">{formatDuration(jobStats.summary.avg_duration)}</p>
                      </div>
                      <Clock className="h-8 w-8 text-muted-foreground" />
                    </div>
                  </CardContent>
                </Card>
              </div>

              {/* Job Type Breakdown */}
              <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
                <Card>
                  <CardHeader>
                    <CardTitle>Fetching Job Stats</CardTitle>
                  </CardHeader>
                  <CardContent className="space-y-4">
                    <div className="grid grid-cols-2 gap-4">
                      <div>
                        <Label className="text-muted-foreground">Total Executions</Label>
                        <p className="text-xl font-bold">{jobStats.by_job_type.fetching?.total_executions || 0}</p>
                      </div>
                      <div>
                        <Label className="text-muted-foreground">Success Rate</Label>
                        <p className="text-xl font-bold">
                          {jobStats.by_job_type.fetching?.total_executions 
                            ? Math.round((jobStats.by_job_type.fetching.successful_executions / jobStats.by_job_type.fetching.total_executions) * 100)
                            : 0
                          }%
                        </p>
                      </div>
                      <div>
                        <Label className="text-muted-foreground">Records Processed</Label>
                        <p className="text-xl font-bold">{jobStats.by_job_type.fetching?.total_records_processed || 0}</p>
                      </div>
                      <div>
                        <Label className="text-muted-foreground">Avg Duration</Label>
                        <p className="text-xl font-bold">{formatDuration(jobStats.by_job_type.fetching?.avg_duration)}</p>
                      </div>
                    </div>
                  </CardContent>
                </Card>

                <Card>
                  <CardHeader>
                    <CardTitle>Syncing Job Stats</CardTitle>
                  </CardHeader>
                  <CardContent className="space-y-4">
                    <div className="grid grid-cols-2 gap-4">
                      <div>
                        <Label className="text-muted-foreground">Total Executions</Label>
                        <p className="text-xl font-bold">{jobStats.by_job_type.syncing?.total_executions || 0}</p>
                      </div>
                      <div>
                        <Label className="text-muted-foreground">Success Rate</Label>
                        <p className="text-xl font-bold">
                          {jobStats.by_job_type.syncing?.total_executions 
                            ? Math.round((jobStats.by_job_type.syncing.successful_executions / jobStats.by_job_type.syncing.total_executions) * 100)
                            : 0
                          }%
                        </p>
                      </div>
                      <div>
                        <Label className="text-muted-foreground">Records Processed</Label>
                        <p className="text-xl font-bold">{jobStats.by_job_type.syncing?.total_records_processed || 0}</p>
                      </div>
                      <div>
                        <Label className="text-muted-foreground">Avg Duration</Label>
                        <p className="text-xl font-bold">{formatDuration(jobStats.by_job_type.syncing?.avg_duration)}</p>
                      </div>
                    </div>
                  </CardContent>
                </Card>
              </div>

              {/* Recent Executions */}
              <Card>
                <CardHeader>
                  <CardTitle>Recent Executions</CardTitle>
                </CardHeader>
                <CardContent>
                  <div className="space-y-2">
                    {jobStats.recent_executions.map((execution, index) => (
                      <div key={index} className="flex items-center justify-between p-2 border rounded">
                        <div className="flex items-center space-x-2">
                          <Badge variant="outline" className="capitalize">
                            {execution.job_type}
                          </Badge>
                          <Badge variant={execution.status === 'completed' ? 'default' : 'destructive'}>
                            {getStatusIcon(execution.status)} {execution.status}
                          </Badge>
                        </div>
                        <div className="flex items-center space-x-4 text-sm">
                          <span>{formatDuration(execution.duration)}</span>
                          <span>{execution.records_processed || 0} records</span>
                          <span className="text-muted-foreground">
                            {new Date(execution.started_at).toLocaleDateString()}
                          </span>
                        </div>
                      </div>
                    ))}
                  </div>
                </CardContent>
              </Card>
            </>
          )}
        </TabsContent>
      </Tabs>
    </div>
  );
}; 