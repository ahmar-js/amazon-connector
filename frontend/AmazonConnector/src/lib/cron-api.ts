// API Configuration
const API_BASE_URL = import.meta.env.VITE_API_BASE_URL || 'http://localhost:8000/api';

// Types for cron job management
export interface CronJobStatus {
  job_type: string;
  status: 'idle' | 'running' | 'completed' | 'failed';
  last_run: string | null;
  next_run: string | null;
  last_duration: string | null;
  error_message: string | null;
}

export interface CronJobConfiguration {
  job_type: string;
  enabled: boolean;
  cron_expression: string;
  description: string;
  date_range_days: number;
  sync_days_back: number;
}

export interface CronJobLog {
  job_type: string;
  status: 'started' | 'completed' | 'failed';
  task_id: string;
  started_at: string;
  completed_at: string | null;
  duration: string | null;
  records_processed: number | null;
  error_message: string | null;
  details: Record<string, any>;
}

export interface CronJobStats {
  summary: {
    total_executions: number;
    successful_executions: number;
    failed_executions: number;
    avg_duration: string;
  };
  by_job_type: {
    [key: string]: {
      total_executions: number;
      successful_executions: number;
      failed_executions: number;
      avg_duration: string;
      total_records_processed: number;
    };
  };
  recent_executions: Array<{
    job_type: string;
    status: string;
    started_at: string;
    duration: string | null;
    records_processed: number | null;
  }>;
  execution_timeline: Array<{
    date: string;
    total_executions: number;
    successful_executions: number;
    failed_executions: number;
  }>;
}

export interface SystemStatus {
  any_job_running: boolean;
  celery_status: {
    active: boolean;
    workers: string[];
  };
  redis_status: {
    active: boolean;
  };
}

export interface CronJobStatusResponse {
  jobs: {
    fetching: {
      status: CronJobStatus;
      configuration: CronJobConfiguration;
    };
    syncing: {
      status: CronJobStatus;
      configuration: CronJobConfiguration;
    };
  };
  system_status: SystemStatus;
}

// API Functions
export class CronJobAPI {
  private static async request(endpoint: string, options: RequestInit = {}) {
    const url = `${API_BASE_URL}${endpoint}`;
    const response = await fetch(url, {
      headers: {
        'Content-Type': 'application/json',
        ...options.headers,
      },
      ...options,
    });

    if (!response.ok) {
      const errorData = await response.json().catch(() => ({}));
      throw new Error(errorData.error || `HTTP error! status: ${response.status}`);
    }

    return response.json();
  }

  // Get status of all cron jobs
  static async getJobStatus(): Promise<CronJobStatusResponse> {
    return this.request('/cron/status/');
  }

  // Get configuration for all jobs or specific job
  static async getJobConfiguration(jobType?: string): Promise<CronJobConfiguration | CronJobConfiguration[]> {
    const endpoint = jobType ? `/cron/config/${jobType}/` : '/cron/config/';
    return this.request(endpoint);
  }

  // Update configuration for a specific job
  static async updateJobConfiguration(jobType: string, config: Partial<CronJobConfiguration>): Promise<{
    success: boolean;
    message: string;
    data: CronJobConfiguration;
  }> {
    return this.request(`/cron/config/${jobType}/`, {
      method: 'PUT',
      body: JSON.stringify(config),
    });
  }

  // Manually trigger a job
  static async triggerJob(jobType: string): Promise<{
    success: boolean;
    message: string;
    task_id: string;
  }> {
    return this.request(`/cron/trigger/${jobType}/`, {
      method: 'POST',
    });
  }

  // Get job execution logs
  static async getJobLogs(jobType?: string, limit = 50, offset = 0): Promise<{
    logs: CronJobLog[];
    pagination: {
      total: number;
      limit: number;
      offset: number;
      has_more: boolean;
    };
  }> {
    const endpoint = jobType 
      ? `/cron/logs/${jobType}/?limit=${limit}&offset=${offset}`
      : `/cron/logs/?limit=${limit}&offset=${offset}`;
    return this.request(endpoint);
  }

  // Get task status
  static async getTaskStatus(taskId: string): Promise<{
    task_id: string;
    status: string;
    result: any;
    traceback: string | null;
    date_done: string | null;
  }> {
    return this.request(`/cron/task/${taskId}/`);
  }

  // Get job statistics
  static async getJobStats(days = 30): Promise<CronJobStats> {
    return this.request(`/cron/stats/?days=${days}`);
  }
}

// Utility functions
export const formatDuration = (duration: string | null): string => {
  if (!duration) return 'N/A';
  
  // Parse duration string (e.g., "0:01:23.456789")
  const parts = duration.split(':');
  if (parts.length >= 3) {
    const hours = parseInt(parts[0]);
    const minutes = parseInt(parts[1]);
    const seconds = Math.floor(parseFloat(parts[2]));
    
    if (hours > 0) {
      return `${hours}h ${minutes}m ${seconds}s`;
    } else if (minutes > 0) {
      return `${minutes}m ${seconds}s`;
    } else {
      return `${seconds}s`;
    }
  }
  
  return duration;
};

export const formatCronExpression = (cronExpr: string): string => {
  // Convert cron expression to human-readable format
  const parts = cronExpr.split(' ');
  if (parts.length !== 5) return cronExpr;
  
  const [minute, hour, dayOfMonth, month, dayOfWeek] = parts;
  
  // Simple conversion for common patterns
  if (cronExpr === '0 0 */15 * *') {
    return 'Every 15 days at midnight';
  } else if (cronExpr === '0 0 */7 * *') {
    return 'Every 7 days at midnight';
  } else if (cronExpr === '0 0 * * *') {
    return 'Daily at midnight';
  } else if (cronExpr === '0 */6 * * *') {
    return 'Every 6 hours';
  } else if (cronExpr === '*/30 * * * *') {
    return 'Every 30 minutes';
  }
  
  return cronExpr;
};

export const getStatusColor = (status: string): string => {
  switch (status) {
    case 'idle':
      return 'text-gray-500';
    case 'running':
      return 'text-blue-500';
    case 'completed':
      return 'text-green-500';
    case 'failed':
      return 'text-red-500';
    default:
      return 'text-gray-500';
  }
};

export const getStatusIcon = (status: string): string => {
  switch (status) {
    case 'idle':
      return '⏸️';
    case 'running':
      return '🔄';
    case 'completed':
      return '✅';
    case 'failed':
      return '❌';
    default:
      return '❓';
  }
}; 