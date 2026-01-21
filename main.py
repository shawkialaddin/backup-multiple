import json
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from time import sleep
from typing import Any, Dict, List, Optional

import requests


UNIT_SECONDS = {
    "second": 1,
    "seconds": 1,
    "minute": 60,
    "minutes": 60,
    "hour": 3600,
    "hours": 3600,
    "day": 86400,
    "days": 86400,
}


@dataclass(frozen=True)
class RetentionConfig:
    value: int
    unit: str

    def to_timedelta(self) -> timedelta:
        unit_key = self.unit.strip().lower()
        if unit_key not in UNIT_SECONDS:
            raise ValueError(f"Invalid retention unit '{self.unit}'. Use seconds, minutes, hours, or days.")
        if self.value <= 0:
            raise ValueError("Retention value must be a positive integer.")
        return timedelta(seconds=self.value * UNIT_SECONDS[unit_key])


@dataclass(frozen=True)
class SourceConfig:
    url: str
    db_password: str

    @staticmethod
    def from_dict(d: Dict[str, Any]) -> "SourceConfig":
        if "url" not in d or "db_password" not in d:
            raise ValueError("Each source must have 'url' and 'db_password'.")
        return SourceConfig(
            url=str(d["url"]).rstrip("/"),
            db_password=str(d["db_password"]),
        )


@dataclass(frozen=True)
class DbBackupConfig:
    db_name: str
    backup_location: str
    prefix: str
    sources: List[SourceConfig]
    retention: Optional[RetentionConfig] = None

    @staticmethod
    def from_dict(d: Dict[str, Any]) -> "DbBackupConfig":
        if "db_name" not in d or "backup_location" not in d:
            raise ValueError("Each system must have 'db_name' and 'backup_location'.")

        prefix = str(d.get("prefix", d.get("perfix", "odoo")))

        sources_raw = d.get("sources")
        if not isinstance(sources_raw, list) or not sources_raw:
            raise ValueError(f"System '{d.get('db_name')}' must have a non-empty 'sources' list.")
        sources = [SourceConfig.from_dict(s) for s in sources_raw]

        retention_cfg = None
        if "retention" in d and isinstance(d["retention"], dict):
            r = d["retention"]
            if "value" in r and "unit" in r:
                retention_cfg = RetentionConfig(value=int(r["value"]), unit=str(r["unit"]))

        return DbBackupConfig(
            db_name=str(d["db_name"]),
            backup_location=str(d["backup_location"]),
            prefix=prefix,
            sources=sources,
            retention=retention_cfg,
        )


class DHInstantOdooDatabaseBackup:
    def __init__(self, config_file_path: str, timeout_seconds: int = 180) -> None:
        self.config_file_path = config_file_path
        self.timeout_seconds = timeout_seconds
        self.systems = self._load_config()

    def _load_config(self) -> List[DbBackupConfig]:
        with open(self.config_file_path, "r", encoding="utf-8") as f:
            cfg = json.load(f)

        systems_raw = cfg.get("systems")
        if not isinstance(systems_raw, list) or not systems_raw:
            raise ValueError("config.json must contain a non-empty list under key 'systems'.")
        return [DbBackupConfig.from_dict(s) for s in systems_raw]

    def _build_output_path(self, system: DbBackupConfig, succeeded_source_url: str) -> Path:
        ts = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
        out_dir = Path(system.backup_location).expanduser().resolve()
        out_dir.mkdir(parents=True, exist_ok=True)

        safe_source = (
            succeeded_source_url.replace("https://", "")
            .replace("http://", "")
            .replace("/", "_")
        )
        filename = f"{system.prefix}_backup_{system.db_name}_{safe_source}_{ts}.zip"
        return out_dir / filename

    def _download_backup(self, db_name: str, source: SourceConfig, out_path: Path) -> None:
        endpoint = f"{source.url}/web/database/backup"
        data = {"master_pwd": source.db_password, "name": db_name, "backup_format": "zip"}

        last_exc: Optional[Exception] = None
        for attempt in range(1, 4):
            try:
                with requests.post(endpoint, data=data, stream=True, timeout=self.timeout_seconds) as r:
                    r.raise_for_status()
                    with open(out_path, "wb") as f:
                        for chunk in r.iter_content(chunk_size=1024 * 1024):
                            if chunk:
                                f.write(chunk)

                if out_path.stat().st_size < 1024:
                    raise RuntimeError(f"Backup file too small: {out_path.stat().st_size} bytes")

                return

            except Exception as exc:
                last_exc = exc
                if out_path.exists():
                    try:
                        out_path.unlink()
                    except OSError:
                        pass
                sleep(2 ** attempt)

        raise RuntimeError(f"Source failed after retries ({source.url}): {last_exc}")

    def cleanup_old_backups(self, system: DbBackupConfig) -> None:
        if not system.retention:
            return

        cutoff = datetime.now() - system.retention.to_timedelta()
        out_dir = Path(system.backup_location).expanduser().resolve()
        if not out_dir.exists():
            return

        deleted = 0
        kept = 0

        # Limit deletion scope to files that match the naming convention for this system
        pattern = f"{system.prefix}_backup_{system.db_name}_*.zip"
        for file_path in out_dir.glob(pattern):
            if not file_path.is_file():
                continue

            mtime = datetime.fromtimestamp(file_path.stat().st_mtime)
            if mtime < cutoff:
                try:
                    file_path.unlink()
                    deleted += 1
                except OSError as e:
                    print(f"  CLEANUP WARNING: Could not delete {file_path.name}: {e}")
            else:
                kept += 1

        print(
            f"  CLEANUP: retention={system.retention.value} {system.retention.unit}, "
            f"deleted={deleted}, remaining_matching_files={kept}"
        )

    def backup_db_with_failover(self, system: DbBackupConfig) -> Optional[Path]:
        for idx, source in enumerate(system.sources, start=1):
            print(f"  Trying source {idx}/{len(system.sources)}: {source.url}")
            out_path = self._build_output_path(system, source.url)
            try:
                self._download_backup(system.db_name, source, out_path)
                print(f"  SUCCESS from {source.url}")
                return out_path
            except Exception as e:
                print(f"  FAILED  from {source.url}: {e}")
        return None

    def execute(self) -> None:
        for system in self.systems:
            print("--------------------------------------------------------------------")
            print(f"STARTED BACKUP for DB '{system.db_name}' using {len(system.sources)} source(s)")

            # Cleanup first (or after backup, either is fine; first keeps disk freer)
            self.cleanup_old_backups(system)

            result = self.backup_db_with_failover(system)
            if result:
                print(f"FINAL RESULT: SUCCESS -> {result}")
            else:
                print("FINAL RESULT: FAILED (all sources failed)")
            print("--------------------------------------------------------------------")


def main() -> None:
    index = 0
    interval_minutes = 1  # for your test scenario

    while True:
        job = DHInstantOdooDatabaseBackup("config.json")
        job.execute()
        index += 1
        print(f"EXECUTED {index} time(s). Sleeping {interval_minutes} minute(s).")
        sleep(interval_minutes * 60)


if __name__ == "__main__":
    main()
