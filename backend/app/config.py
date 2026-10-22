class Settings:

    # Persistence
    persistence_probe_interval = 5

    # Workers
    worker_count = 2
    recovery_worker_count = 2

    # Heartbeats
    heartbeat_interval = 5
    worker_timeout = 15

    recovery_heartbeat_interval = 5
    recovery_worker_timeout = 15
    recovery_supervisor_check_interval = 1

    # Recovery
    failed_retry_interval = 10
    limited_recovery_delay = 2

    # Supervisors
    supervisor_check_interval = 1

    # Respawning
    max_respawns = 3
    respawn_window_seconds = 10

    max_recovery_respawns = 3
    recovery_respawn_window_seconds = 10

    restart_backoff_seconds = 2

    # Persistence retries
    max_retries = 3
    retry_delay_seconds = 1


settings = Settings()