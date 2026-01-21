import json
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from time import sleep
from typing import Any, Dict, List, Optional

import requests


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

    @staticmethod
    def from_dict(d: Dict[str, Any]) -> "DbBackupConfig":
        if "db_name" not in d or "backup_location" not in d:
            raise ValueError("Each system must have 'db_name' and 'backup_location'.")

        prefix = str(d.get("prefix", d.get("perfix", "odoo")))
        sources_raw = d.get("sources")
        if not isinstance(sources_raw, list) or not sources_raw:
            raise ValueError(f"System '{d.get('db_name')}' must have a non-empty 'sources' list.")

        sources = [SourceConfig.from_dict(s) for s in sources_raw]
        return DbBackupConfig(
            db_name=str(d["db_name"]),
            backup_location=str(d["backup_location"]),
            prefix=prefix,
            sources=sources,
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

        safe_source = succeeded_source_url.replace("https://", "").replace("http://", "").replace("/", "_")
        filename = f"{system.prefix}_backup_{system.db_name}_{safe_source}_{ts}.zip"
        return out_dir / filename

    def _download_backup(self, db_name: str, source: SourceConfig, out_path: Path) -> None:
        endpoint = f"{source.url}/web/database/backup"
        data = {
            "master_pwd": source.db_password,
            "name": db_name,
            "backup_format": "zip",
        }

        # Retry only within a source; if it still fails, we move to next source
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

    def backup_db_with_failover(self, system: DbBackupConfig) -> Optional[Path]:
        """
        Try all sources in order. Stop on first success and do not try remaining sources.
        Return the backup path on success, else None.
        """
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
            result = self.backup_db_with_failover(system)
            if result:
                print(f"FINAL RESULT: SUCCESS -> {result}")
            else:
                print("FINAL RESULT: FAILED (all sources failed)")
            print("--------------------------------------------------------------------")


def main() -> None:
    index = 0
    interval_minutes = 60  # adjust

    while True:
        job = DHInstantOdooDatabaseBackup("config.json")
        job.execute()
        index += 1
        print(f"EXECUTED {index} time(s). Sleeping {interval_minutes} minute(s).")
        sleep(interval_minutes * 60)


if __name__ == "__main__":
    main()
