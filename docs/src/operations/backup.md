# Backup

Noxu DB supports live backup via the `BackupManager` daemon, which copies
log files to a target directory without pausing writes.

```rust
// Enable automatic backup in EnvironmentConfig
EnvironmentConfig::new(path)
    .with_backup_dir(Some(PathBuf::from("/backup/noxu")))
    .with_backup_interval_ms(Some(300_000))  // every 5 minutes
```

The backup directory will contain a complete, consistent snapshot that can be
used to start a new environment. No additional tooling is required.

## Recovery from Backup

Copy the backup directory to a new location and open it as a normal environment.
Noxu DB will perform normal recovery on open.
