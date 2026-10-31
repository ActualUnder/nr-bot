from __future__ import annotations

import os
import re
from dataclasses import dataclass
from pathlib import Path


def flag(name: str, default: bool = True) -> bool:
    return os.getenv(name, str(default)).lower() in {"1", "true", "yes", "on"}


def number(name: str, default: float, low: float, high: float) -> float:
    value = float(os.getenv(name, str(default)))
    if not low <= value <= high:
        raise ValueError(f"{name} must be between {low} and {high}")
    return value


AREA_NAMES = {
    "T0": "Ashington", "T1": "Newcastle & Gateshead",
    "T2": "Darlington approaches", "T3": "Sunderland & Pelaw",
}


@dataclass(frozen=True)
class AdvancedConfig:
    data_dir: Path
    app_dir: Path
    areas: tuple[str, ...] = ("T0", "T1", "T2", "T3")
    enabled: bool = True
    trust_enabled: bool = True
    tsr_enabled: bool = True
    vstp_enabled: bool = True
    reference_enabled: bool = True
    map_enabled: bool = True
    map_host: str = "127.0.0.1"
    map_port: int = 8790
    map_token: str = ""
    map_public_url: str = ""
    stale_seconds: float = 180
    berth_max_age: float = 1800
    baseline_min: int = 8
    slow_ratio: float = 1.8
    slow_extra: float = 45
    trust_delay_minutes: float = 2
    retention_days: int = 14
    trust_clock: str = "network-rail"

    @property
    def db_path(self) -> Path:
        return self.data_dir / "network.sqlite"

    @property
    def topics(self) -> dict[str, str]:
        return {name: topic for name, topic, enabled in (
            ("trust", "/topic/TRAIN_MVT_ALL_TOC", self.trust_enabled),
            ("tsr", "/topic/TSR_ALL_ROUTE", self.tsr_enabled),
            ("vstp", "/topic/VSTP_ALL", self.vstp_enabled),
        ) if enabled}

    @classmethod
    def from_env(cls, data_root: Path, app_dir: Path) -> "AdvancedConfig":
        areas = tuple(dict.fromkeys(x.strip().upper() for x in
                                   os.getenv("NR_AREAS", "T0,T1,T2,T3").split(",") if x.strip()))
        if not areas or any(not re.fullmatch(r"[A-Z0-9]{2}", a) for a in areas):
            raise ValueError("NR_AREAS must be comma-separated two-character TD area IDs")
        clock = os.getenv("NR_TRUST_CLOCK", "network-rail")
        if clock not in {"network-rail", "utc"}:
            raise ValueError("NR_TRUST_CLOCK must be network-rail or utc")
        return cls(
            data_dir=data_root / "network", app_dir=app_dir, areas=areas,
            enabled=flag("NR_ADVANCED_ENABLED"), trust_enabled=flag("NR_TRUST_ENABLED"),
            tsr_enabled=flag("NR_TSR_ENABLED"), vstp_enabled=flag("NR_VSTP_ENABLED"),
            reference_enabled=flag("NR_REFERENCE_AUTO_REFRESH"), map_enabled=flag("NR_MAP_ENABLED"),
            map_host=os.getenv("NR_MAP_BIND", "127.0.0.1"),
            map_port=int(number("NR_MAP_PORT", 8790, 1, 65535)),
            map_token=os.getenv("NR_MAP_TOKEN", ""),
            map_public_url=os.getenv("NR_MAP_PUBLIC_URL", "").rstrip("/"),
            stale_seconds=number("NR_NETWORK_STALE_SECONDS", 180, 30, 3600),
            berth_max_age=number("NR_BERTH_MAX_AGE_SECONDS", 1800, 180, 86400),
            baseline_min=int(number("NR_BASELINE_MIN_SAMPLES", 8, 3, 100)),
            slow_ratio=number("NR_SLOW_RATIO", 1.8, 1.2, 10),
            slow_extra=number("NR_SLOW_EXTRA_SECONDS", 45, 10, 600),
            trust_delay_minutes=number("NR_TRUST_DELAY_GAIN_MINUTES", 2, 1, 30),
            retention_days=int(number("NR_NETWORK_RETENTION_DAYS", 14, 2, 90)),
            trust_clock=clock,
        )
