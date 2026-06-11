"""Environment (.env, immutable) and runtime settings (DB-persisted, Telegram-adjustable).

`RANGES` is the single source of truth for /set validation and bootstrap defaults.
"""
from __future__ import annotations

import json
import os
from dataclasses import dataclass
from typing import Any, Optional

from dotenv import load_dotenv
from pydantic import BaseModel, ConfigDict

from core import timewin

MAINNET_API_URL = "https://api.hyperliquid.xyz"
TESTNET_API_URL = "https://api.hyperliquid-testnet.xyz"

# Paper-mode fallback fee rates, used ONLY when live fee rates cannot be fetched
# (a warning is logged). Live mode always reads rates from the userFees endpoint.
FALLBACK_MAKER_FEE = 0.00015   # 0.015 %
FALLBACK_TAKER_FEE = 0.00045   # 0.045 %


@dataclass(frozen=True)
class Env:
    hl_agent_private_key: str
    hl_main_wallet_address: str
    hl_subaccount_address: Optional[str]
    hl_network: str
    tg_bot_token: Optional[str]
    tg_owner_id: Optional[int]
    tg_channel_id: Optional[str]
    dash_bearer_token: str
    mode: str
    paper_start_equity: float
    db_path: str
    log_dir: str
    dash_host: str
    dash_port: int
    dash_public: bool = False        # True = dashboard open without bearer token
    dash_public_url: str = ""        # optional fixed URL reported by /dashboard

    @property
    def api_url(self) -> str:
        return TESTNET_API_URL if self.hl_network == "testnet" else MAINNET_API_URL

    @property
    def trading_address(self) -> str:
        """Address whose positions/equity/fills the bot manages (subaccount if set)."""
        return self.hl_subaccount_address or self.hl_main_wallet_address

    @classmethod
    def load(cls, dotenv_path: str = ".env") -> "Env":
        load_dotenv(dotenv_path, override=False)

        def _opt(name: str) -> Optional[str]:
            v = os.environ.get(name, "").strip()
            return v or None

        owner = _opt("TG_OWNER_ID")
        return cls(
            hl_agent_private_key=os.environ.get("HL_AGENT_PRIVATE_KEY", "").strip(),
            hl_main_wallet_address=os.environ.get("HL_MAIN_WALLET_ADDRESS", "").strip(),
            hl_subaccount_address=_opt("HL_SUBACCOUNT_ADDRESS"),
            hl_network=os.environ.get("HL_NETWORK", "mainnet").strip().lower(),
            tg_bot_token=_opt("TG_BOT_TOKEN"),
            tg_owner_id=int(owner) if owner else None,
            tg_channel_id=_opt("TG_CHANNEL_ID"),
            dash_bearer_token=os.environ.get("DASH_BEARER_TOKEN", "").strip(),
            mode=os.environ.get("MODE", "paper").strip().lower(),
            paper_start_equity=float(os.environ.get("PAPER_START_EQUITY", "10000")),
            db_path=os.environ.get("DB_PATH", "data/flowscalp.db").strip(),
            log_dir=os.environ.get("LOG_DIR", "logs").strip(),
            dash_host=os.environ.get("DASH_HOST", "127.0.0.1").strip(),
            dash_port=int(os.environ.get("DASH_PORT", "8080")),
            dash_public=os.environ.get("DASH_PUBLIC", "").strip().lower() in _TRUE,
            dash_public_url=os.environ.get("DASH_PUBLIC_URL", "").strip().rstrip("/"),
        )


class Settings(BaseModel):
    model_config = ConfigDict(frozen=True)

    risk_pct: float = 0.5
    sl_cap_pct: float = 0.5
    sl_buffer_pct: float = 0.05
    tp_r: float = 1.8
    time_stop_min: int = 15
    leverage_cap: int = 5
    daily_loss_limit_pct: float = 3.0
    max_consec_losses: int = 4
    cooldown_min: int = 120
    atr_floor_bps: float = 8
    atr_ceil_bps: float = 60
    sweep_lookback: int = 30
    sweep_min_pen_pct: float = 0.05
    maker_timeout_s: int = 20
    allow_taker_entry: bool = False
    sweep_vol_mult: float = 1.7
    cvd_filter: bool = True
    session_windows: tuple[str, ...] = ("07:00-20:00",)
    blackout_windows: tuple[str, ...] = ("13:15-13:45", "18:45-19:15")
    funding_filter: bool = False
    bias_filter: bool = True


RANGES: dict[str, dict[str, Any]] = {
    "risk_pct":             {"type": float, "min": 0.1, "max": 2.0,
                             "desc": "persen equity yang dirisikokan per trade"},
    "sl_cap_pct":           {"type": float, "min": 0.2, "max": 1.0,
                             "desc": "jarak SL maksimum %; butuh SL lebih lebar = trade dilewati"},
    "sl_buffer_pct":        {"type": float, "min": 0.02, "max": 0.15,
                             "desc": "buffer SL % di luar ujung wick sweep"},
    "tp_r":                 {"type": float, "min": 1.0, "max": 3.0,
                             "desc": "target profit dalam kelipatan R (risiko SL)"},
    "time_stop_min":        {"type": int, "min": 5, "max": 60,
                             "desc": "tutup paksa setelah N menit jika SL/TP belum tersentuh"},
    "leverage_cap":         {"type": int, "min": 1, "max": 10,
                             "desc": "leverage efektif maksimum (ukuran posisi diperkecil)"},
    "daily_loss_limit_pct": {"type": float, "min": 1.0, "max": 10.0,
                             "desc": "rugi harian % yang menyetop entry sampai 00:00 UTC"},
    "max_consec_losses":    {"type": int, "min": 2, "max": 10,
                             "desc": "kalah beruntun sebanyak ini = masuk cooldown"},
    "cooldown_min":         {"type": int, "min": 30, "max": 480,
                             "desc": "lama cooldown dalam menit"},
    "atr_floor_bps":        {"type": float, "min": 2.0, "max": 50.0,
                             "desc": "skip entry saat ATR 1m di bawah ini (pasar mati)"},
    "atr_ceil_bps":         {"type": float, "min": 20.0, "max": 200.0,
                             "desc": "skip entry saat ATR 1m di atas ini (pasar liar/berita)"},
    "sweep_lookback":       {"type": int, "min": 10, "max": 120,
                             "desc": "jumlah bar 1m penentu swing high/low yang disapu"},
    "sweep_min_pen_pct":    {"type": float, "min": 0.01, "max": 0.2,
                             "desc": "penetrasi wick minimum % melewati level"},
    "maker_timeout_s":      {"type": int, "min": 5, "max": 60,
                             "desc": "batalkan entry maker yang tak terisi setelah N detik"},
    "allow_taker_entry":    {"type": bool,
                             "desc": "kejar pakai market order saat entry maker gagal"},
    "sweep_vol_mult":       {"type": float, "min": 1.0, "max": 3.0,
                             "desc": "volume bar sweep harus >= ini x rata-rata 20 bar; 1.0 = mati"},
    "cvd_filter":           {"type": bool,
                             "desc": "wajib ada penyerapan order flow (divergensi CVD) saat sweep"},
    "session_windows":      {"type": "windows",
                             "desc": "jam boleh entry (UTC); di luar ini tidak ada entry baru"},
    "blackout_windows":     {"type": "windows",
                             "desc": "jendela rilis berita (UTC); tidak entry di dalamnya"},
    "funding_filter":       {"type": bool,
                             "desc": "hindari sisi yang melawan funding yang sudah penuh sesak"},
    "bias_filter":          {"type": bool,
                             "desc": "filter tren EMA200 15m (di atas = long saja, di bawah = short)"},
}

# Human-friendly button/display labels (Indonesian) for each setting key
LABELS: dict[str, str] = {
    "risk_pct": "Risiko per trade (%)",
    "sl_cap_pct": "Batas jarak SL (%)",
    "sl_buffer_pct": "Buffer SL (%)",
    "tp_r": "Target profit (R)",
    "time_stop_min": "Batas waktu posisi (menit)",
    "leverage_cap": "Leverage maksimum",
    "daily_loss_limit_pct": "Batas rugi harian (%)",
    "max_consec_losses": "Maks kalah beruntun",
    "cooldown_min": "Lama istirahat (menit)",
    "atr_floor_bps": "Volatilitas minimum (bps)",
    "atr_ceil_bps": "Volatilitas maksimum (bps)",
    "sweep_lookback": "Lookback sweep (bar)",
    "sweep_min_pen_pct": "Penetrasi sweep min (%)",
    "maker_timeout_s": "Timeout entry (detik)",
    "allow_taker_entry": "Kejar entry pakai market",
    "sweep_vol_mult": "Filter volume sweep (x)",
    "cvd_filter": "Filter order flow (CVD)",
    "session_windows": "Jam trading (UTC)",
    "blackout_windows": "Jam hindari berita (UTC)",
    "funding_filter": "Filter funding",
    "bias_filter": "Filter tren 15m",
}

_TRUE = {"true", "1", "on", "yes"}
_FALSE = {"false", "0", "off", "no"}


def validate_setting(key: str, raw: Any) -> tuple[bool, Any]:
    """Validate one /set input against RANGES. Returns (ok, parsed_value | error_msg)."""
    spec = RANGES.get(key)
    if spec is None:
        return False, f"unknown key {key!r}"
    t = spec["type"]
    if t is bool:
        s = str(raw).strip().lower()
        if s in _TRUE:
            return True, True
        if s in _FALSE:
            return True, False
        return False, f"{key}: expected true/false, got {raw!r}"
    if t is int or t is float:
        try:
            v = t(float(raw)) if t is int else float(raw)
        except (TypeError, ValueError):
            return False, f"{key}: not a number: {raw!r}"
        if t is int and float(raw) != int(float(raw)):
            return False, f"{key}: expected integer, got {raw!r}"
        if not (spec["min"] <= v <= spec["max"]):
            return False, f"{key}: {v} out of range [{spec['min']}–{spec['max']}]"
        return True, v
    if t == "windows":
        if isinstance(raw, (list, tuple)):
            items = [str(x) for x in raw]
        else:
            s = str(raw).strip()
            if s.startswith("["):
                try:
                    parsed = json.loads(s)
                    if not isinstance(parsed, list):
                        raise ValueError
                    items = [str(x) for x in parsed]
                except ValueError:
                    return False, f"{key}: invalid JSON list: {raw!r}"
            else:
                items = [p.strip() for p in s.split(",") if p.strip()]
        try:
            for w in items:
                timewin.parse_window(w)
        except ValueError as e:
            return False, f"{key}: {e}"
        return True, tuple(items)
    return False, f"{key}: unsupported type"  # pragma: no cover


def update_env_file(values: dict[str, str], path: str = ".env") -> None:
    """Atomically upsert KEY=VALUE lines in the .env file, keeping 600 perms."""
    lines: list[str] = []
    if os.path.exists(path):
        with open(path) as f:
            lines = f.read().splitlines()
    seen: set[str] = set()
    out: list[str] = []
    for ln in lines:
        key = ln.split("=", 1)[0].strip() if ("=" in ln and not ln.lstrip().startswith("#")) else None
        if key in values:
            out.append(f"{key}={values[key]}")
            seen.add(key)
        else:
            out.append(ln)
    for k, v in values.items():
        if k not in seen:
            out.append(f"{k}={v}")
    tmp = path + ".tmp"
    with open(tmp, "w") as f:
        f.write("\n".join(out) + "\n")
    os.chmod(tmp, 0o600)
    os.replace(tmp, path)


class SettingsStore:
    """DB-backed runtime settings with an in-memory cache."""

    def __init__(self, db):
        self._db = db
        self._cache = Settings()

    @property
    def current(self) -> Settings:
        return self._cache

    async def load(self) -> Settings:
        stored = await self._db.all_settings()
        values: dict[str, Any] = {}
        defaults = Settings()
        for key in RANGES:
            if key in stored:
                values[key] = json.loads(stored[key])
            else:
                default = getattr(defaults, key)
                values[key] = default
                await self._db.set_setting(key, json.dumps(list(default) if isinstance(default, tuple) else default))
        self._cache = Settings(**values)
        return self._cache

    async def get(self) -> Settings:
        return self._cache

    async def set(self, key: str, raw: Any) -> tuple[bool, Any, Any]:
        """Returns (ok, old_value, new_value | error_msg)."""
        old = getattr(self._cache, key, None)
        ok, parsed = validate_setting(key, raw)
        if not ok:
            return False, old, parsed
        if key in ("atr_floor_bps", "atr_ceil_bps"):
            floor = parsed if key == "atr_floor_bps" else self._cache.atr_floor_bps
            ceil = parsed if key == "atr_ceil_bps" else self._cache.atr_ceil_bps
            if floor >= ceil:
                return False, old, f"atr_floor_bps ({floor}) must stay below atr_ceil_bps ({ceil})"
        self._cache = self._cache.model_copy(update={key: parsed})
        await self._db.set_setting(key, json.dumps(list(parsed) if isinstance(parsed, tuple) else parsed))
        return True, old, parsed
