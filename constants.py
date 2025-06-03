"""
Constants for job states and status mappings.
"""

from job_system.base import JobStatus

# State constants for job workflow
STATE_PENDING = 0
STATE_RUNNING = 1
STATE_COMPLETED = 2
STATE_FAILED = 3
STATE_EXTERNAL_FAILED = 4

# Status mapping for backward compatibility with existing logs
STATUS_DICT = {
    STATE_PENDING: 'pending',
    STATE_RUNNING: 'running',
    STATE_COMPLETED: 'done',
    STATE_FAILED: 'failed',
    STATE_EXTERNAL_FAILED: 'external failed'
}

# Status mapping from new JobStatus to old numeric status
JOB_STATUS_MAPPING = {
    JobStatus.PENDING: STATE_PENDING,
    JobStatus.RUNNING: STATE_RUNNING,
    JobStatus.COMPLETED: STATE_COMPLETED,
    JobStatus.FAILED: STATE_FAILED,
    JobStatus.CANCELLED: STATE_EXTERNAL_FAILED,
    JobStatus.TIMEOUT: STATE_EXTERNAL_FAILED,
    JobStatus.UNKNOWN: STATE_EXTERNAL_FAILED
} 