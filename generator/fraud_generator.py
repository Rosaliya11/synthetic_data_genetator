"""
Synthetic Fraud Generator - Core engine.
Produces realistic, privacy-safe fraud datasets for education and ML.
"""

import random
import hashlib
from datetime import datetime, timedelta, date
from typing import Optional, List, Dict
from dataclasses import dataclass, field
import json
import csv
import io

from .db import save_to_sqlite


# Realistic feature constants (privacy-safe, synthetic)
CHANNELS = ["Mobile App", "Online Banking", "ATM", "POS", "Branch", "UPI"]
OCCUPATIONS = [
    "Engineer", "Teacher", "Doctor", "Accountant", "Manager", "Developer",
    "Nurse", "Driver", "Sales", "Designer", "Analyst", "Consultant", "Retired", "Student"
]
LOCATION_CITIES = [
    ("Mumbai", "IN"), ("Delhi", "IN"), ("Bangalore", "IN"), ("Chennai", "IN"), ("Kolkata", "IN"),
    ("Hyderabad", "IN"), ("Pune", "IN"), ("Ahmedabad", "IN"), ("Jaipur", "IN"), ("Lucknow", "IN"),
    ("London", "UK"), ("Manchester", "UK"), ("Dubai", "AE"), ("Singapore", "SG"), ("New York", "US"),
]


@dataclass
class GeneratorConfig:
    """Configuration for synthetic fraud dataset generation."""
    num_transactions: int = 1000
    fraud_ratio: float = 0.02  # 2% fraud by default
    seed: Optional[int] = None
    include_balance_fields: bool = True
    include_timestamps: bool = True
    include_realistic_features: bool = True  # TransactionType, Location, DeviceID, IP, etc.
    transaction_types: list = field(default_factory=lambda: ["PAYMENT", "TRANSFER", "CASH_OUT", "DEBIT", "CASH_IN"])
    # Fraud behavior tuning
    fraud_prefers_transfer: bool = True
    fraud_prefers_cash_out: bool = True
    fraud_amount_multiplier: float = 1.5  # fraud often larger
    timezone_offset_hours: int = 0


@dataclass
class BehavioralConfig:
    """
    Configuration for behavior-driven transaction generation.

    Simulates users with long-term patterns (salary + weekend spend)
    over a period of months, then flags deviations as fraud.
    """

    num_users: int = 100
    months: int = 18  # 12–24 months typical
    seed: Optional[int] = None

    # Salary behaviour (amounts in INR)
    salary_min: float = 30_000
    salary_max: float = 100_000
    salary_noise: float = 0.05  # ±5% noise

    # Weekend spend behaviour (per weekend)
    weekend_min: float = 5_000
    weekend_max: float = 20_000
    weekend_noise: float = 0.25

    # Anomaly / fraud thresholds (multipliers on user means)
    salary_anomaly_min_mult: float = 5.0
    salary_anomaly_max_mult: float = 40.0
    weekend_anomaly_min_mult: float = 5.0
    weekend_anomaly_max_mult: float = 40.0

    # Probability of injecting an anomaly per pattern event
    salary_anomaly_prob: float = 0.02
    weekend_anomaly_prob: float = 0.01

    include_balance_fields: bool = True
    include_timestamps: bool = True
    timezone_offset_hours: int = 0

    # Optional SQLite persistence
    persist_profiles: bool = False
    profiles_db_path: str = "data/behavior_profiles.db"
    profiles_table: str = "behavior_profiles"


def _synthetic_id(prefix: str, index: int, seed: int) -> str:
    """Generate a deterministic, privacy-safe account-like ID."""
    raw = f"{prefix}_{seed}_{index}"
    return "C" + hashlib.sha256(raw.encode()).hexdigest()[:10]


def _synthetic_name(index: int, seed: int, is_origin: bool) -> str:
    """Origin/destination name (customer or merchant style)."""
    pre = "orig" if is_origin else "dest"
    raw = f"{pre}_{seed}_{index}"
    return hashlib.sha256(raw.encode()).hexdigest()[:12]


def _synthetic_device_id(seed: int, index: int, rng: random.Random) -> str:
    """Privacy-safe device identifier."""
    raw = f"dev_{seed}_{index}_{rng.randint(0, 999999)}"
    return "DEV" + hashlib.sha256(raw.encode()).hexdigest()[:14]


def _synthetic_ip(seed: int, index: int, rng: random.Random) -> str:
    """Synthetic IP (non-routable / random dotted quad)."""
    a = rng.choice([10, 172, 192, 203, 45])
    if a == 10:
        b, c, d = rng.randint(0, 255), rng.randint(0, 255), rng.randint(1, 254)
    else:
        b, c, d = rng.randint(0, 255), rng.randint(0, 255), rng.randint(0, 255)
    return f"{a}.{b}.{c}.{d}"


def _synthetic_merchant_id(seed: int, index: int, rng: random.Random) -> str:
    """Privacy-safe merchant identifier."""
    raw = f"merchant_{seed}_{index}_{rng.randint(1000, 99999)}"
    return "MCH" + hashlib.sha256(raw.encode()).hexdigest()[:12]


def _synthetic_location(rng: random.Random) -> str:
    """City, Country code."""
    city, cc = rng.choice(LOCATION_CITIES)
    return f"{city},{cc}"


def _synthetic_channel(rng: random.Random, is_fraud: bool) -> str:
    """Channel; fraud slightly more often from unusual channels."""
    if is_fraud and rng.random() < 0.2:
        return rng.choice(["Mobile App", "Online Banking"])  # often same, but different IP/device
    return rng.choice(CHANNELS)


def _previous_transaction_date(current_ts: datetime, rng: random.Random) -> str:
    """Previous tx timestamp (1 hour to 30 days before)."""
    delta_sec = rng.randint(3600, 30 * 24 * 3600)
    prev = current_ts - timedelta(seconds=delta_sec)
    return prev.isoformat()


class SyntheticFraudGenerator:
    """
    Generates realistic synthetic transaction data with configurable fraud.
    All data is fake; no real PII is used.
    """

    def __init__(self, config: Optional[GeneratorConfig] = None):
        self.config = config or GeneratorConfig()
        self.rng = random.Random(self.config.seed)

    def generate(self) -> list[dict]:
        """Generate one dataset of transactions with fraud labels."""
        cfg = self.config
        if cfg.seed is not None:
            self.rng = random.Random(cfg.seed)

        n = cfg.num_transactions
        n_fraud = max(0, min(n, int(round(n * cfg.fraud_ratio))))
        n_legit = n - n_fraud

        # Pre-generate pool of synthetic account indices
        max_accounts = max(500, n // 3)
        origin_indices = list(range(max_accounts))
        dest_indices = list(range(max_accounts))
        self.rng.shuffle(origin_indices)
        self.rng.shuffle(dest_indices)

        types = list(cfg.transaction_types)
        fraud_types = []
        if "TRANSFER" in types and cfg.fraud_prefers_transfer:
            fraud_types.append("TRANSFER")
        if "CASH_OUT" in types and cfg.fraud_prefers_cash_out:
            fraud_types.append("CASH_OUT")
        if "DEBIT" in types:
            fraud_types.append("DEBIT")
        if not fraud_types:
            fraud_types = types

        out: list[dict] = []
        base_time = datetime(2024, 1, 1) + timedelta(hours=cfg.timezone_offset_hours)

        # Legitimate transactions
        for i in range(n_legit):
            step = i + 1
            t_type = self.rng.choice(types)
            amount = round(self.rng.uniform(1, 5000), 2)
            o_idx = self.rng.choice(origin_indices)
            d_idx = self.rng.choice(dest_indices)
            if o_idx == d_idx:
                d_idx = (d_idx + 1) % max_accounts
            name_orig = _synthetic_name(o_idx, cfg.seed or 0, True)
            name_dest = _synthetic_name(d_idx, cfg.seed or 0, False)
            row = self._make_row(
                step=step, t_type=t_type, amount=amount,
                name_orig=name_orig, name_dest=name_dest,
                is_fraud=False, base_time=base_time,
                customer_idx=o_idx, merchant_idx=d_idx,
            )
            out.append(row)

        # Fraudulent transactions
        for i in range(n_fraud):
            step = n_legit + i + 1
            t_type = self.rng.choice(fraud_types)
            amount = round(self.rng.uniform(10, 8000) * cfg.fraud_amount_multiplier, 2)
            o_idx = self.rng.choice(origin_indices)
            d_idx = self.rng.choice(dest_indices)
            if o_idx == d_idx:
                d_idx = (d_idx + 1) % max_accounts
            name_orig = _synthetic_name(o_idx, (cfg.seed or 0) + 1, True)
            name_dest = _synthetic_name(d_idx, (cfg.seed or 0) + 1, False)
            row = self._make_row(
                step=step, t_type=t_type, amount=amount,
                name_orig=name_orig, name_dest=name_dest,
                is_fraud=True, base_time=base_time,
                customer_idx=o_idx, merchant_idx=d_idx,
            )
            out.append(row)

        self.rng.shuffle(out)
        # Re-number step after shuffle to keep order
        for i, row in enumerate(out, 1):
            row["step"] = i
        return out

    def _make_row(
        self,
        step: int,
        t_type: str,
        amount: float,
        name_orig: str,
        name_dest: str,
        is_fraud: bool,
        base_time: datetime,
        customer_idx: int = 0,
        merchant_idx: int = 0,
    ) -> dict:
        cfg = self.config
        row = {
            "step": step,
            "type": t_type,
            "TransactionType": t_type,
            "amount": amount,
            "nameOrig": name_orig,
            "nameDest": name_dest,
        }
        ts = base_time + timedelta(minutes=step * self.rng.randint(1, 15))
        if cfg.include_timestamps:
            row["timestamp"] = ts.isoformat()
            row["hour_of_day"] = ts.hour
            row["day_of_week"] = ts.weekday()
        if cfg.include_balance_fields:
            old_orig = round(self.rng.uniform(5000, 150000), 2)
            old_dest = round(self.rng.uniform(0, 50000), 2)
            row["oldbalanceOrg"] = old_orig
            row["oldbalanceDest"] = old_dest
            row["AccountBalance"] = old_orig
        row["TransactionDuration"] = self.rng.randint(8, 180) if not is_fraud else self.rng.choice([1, 2, 3, 4, 5, 300, 450, 600])
        if cfg.include_realistic_features:
            row["Location"] = _synthetic_location(self.rng)
            row["DeviceID"] = _synthetic_device_id(cfg.seed or 0, customer_idx, self.rng)
            row["IPAddress"] = _synthetic_ip(cfg.seed or 0, customer_idx * 1000 + step, self.rng)
            row["MerchantID"] = _synthetic_merchant_id(cfg.seed or 0, merchant_idx, self.rng)
            row["Channel"] = _synthetic_channel(self.rng, is_fraud)
            row["CustomerAge"] = self.rng.randint(22, 68)
            row["CustomerOccupation"] = self.rng.choice(OCCUPATIONS)
            row["LoginAttempts"] = self.rng.randint(1, 2) if not is_fraud else self.rng.randint(1, 8)
            row["PreviousTransactionDate"] = _previous_transaction_date(ts, self.rng)
            if "AccountBalance" not in row:
                row["AccountBalance"] = round(self.rng.uniform(5000, 150000), 2)
        return row

    def to_csv(self, data: Optional[list[dict]] = None) -> str:
        """Export generated data to CSV string."""
        data = data or self.generate()
        if not data:
            return ""
        buf = io.StringIO()
        writer = csv.DictWriter(buf, fieldnames=list(data[0].keys()))
        writer.writeheader()
        writer.writerows(data)
        return buf.getvalue()

    def to_json(self, data: Optional[list[dict]] = None) -> str:
        """Export generated data to JSON string."""
        data = data or self.generate()
        return json.dumps(data, indent=2)


class BehavioralFraudGenerator:
    """
    Behavior-driven generator that simulates user-centric patterns
    (salary credits, weekend spending) over many months and flags
    deviations as fraud.
    """

    def __init__(self, config: Optional[BehavioralConfig] = None):
        self.config = config or BehavioralConfig()
        self.rng = random.Random(self.config.seed)

    def _user_id(self, idx: int) -> str:
        # Stable, hashed, privacy-safe user identifier
        seed = self.config.seed or 0
        raw = f"user_{seed}_{idx}"
        return hashlib.sha256(raw.encode()).hexdigest()[:16]

    def _base_date(self) -> date:
        # Start at a fixed point for reproducibility
        return date(2023, 1, 1)

    def generate(self) -> List[Dict]:
        """
        Generate a behavior-driven dataset.

        Each user has:
          - Monthly salary pattern
          - Weekend spending pattern
        Metadata per transaction:
          - pattern_type (Salary Credit / Weekend Spending / Anomaly)
          - hist_mean, hist_freq, deviation_score
        """
        cfg = self.config
        if cfg.seed is not None:
            self.rng = random.Random(cfg.seed)

        base_d = self._base_date()
        # Pre-build per-user behaviour configs
        users = []
        for u in range(cfg.num_users):
            salary_base = self.rng.uniform(cfg.salary_min, cfg.salary_max)
            weekend_base = self.rng.uniform(cfg.weekend_min, cfg.weekend_max)
            users.append(
                {
                    "user_id": self._user_id(u),
                    "salary_base": salary_base,
                    "weekend_base": weekend_base,
                }
            )

        records: List[Dict] = []

        # Simulate month by month for each user
        for u_info in users:
            uid = u_info["user_id"]
            salary_hist: List[float] = []
            weekend_hist: List[float] = []

            for m in range(cfg.months):
                # Approximate month offset as 30 days
                month_start = base_d + timedelta(days=30 * m)

                # --- Salary credit (normal pattern) ---
                salary_day = self.rng.randint(1, 5)
                salary_amount = u_info["salary_base"] * (
                    1 + self.rng.uniform(-cfg.salary_noise, cfg.salary_noise)
                )
                salary_amount = round(max(0, salary_amount), 2)
                salary_date = datetime(
                    year=month_start.year,
                    month=month_start.month,
                    day=min(salary_day, 28),
                    hour=self.rng.randint(9, 12),
                ) + timedelta(hours=cfg.timezone_offset_hours)

                salary_hist.append(salary_amount)
                records.append(
                    self._make_behavior_row(
                        uid,
                        when=salary_date,
                        amount=salary_amount,
                        pattern_type="Salary Credit",
                        pattern_group="salary",
                        hist=salary_hist,
                        is_anomaly=False,
                    )
                )

                # Optional salary anomaly
                if self.rng.random() < cfg.salary_anomaly_prob:
                    mult = self.rng.uniform(
                        cfg.salary_anomaly_min_mult,
                        cfg.salary_anomaly_max_mult,
                    )
                    anomaly_amount = round(salary_amount * mult, 2)
                    anomaly_date = salary_date + timedelta(
                        hours=self.rng.randint(0, 24)
                    )
                    salary_hist.append(anomaly_amount)
                    records.append(
                        self._make_behavior_row(
                            uid,
                            when=anomaly_date,
                            amount=anomaly_amount,
                            pattern_type="Salary Anomaly",
                            pattern_group="salary",
                            hist=salary_hist,
                            is_anomaly=True,
                        )
                    )

                # --- Weekend spending pattern ---
                # Simulate 4 weekends per month
                for w in range(4):
                    # Use Saturday (5) or Sunday (6)
                    weekend_day = self.rng.choice([5, 6])
                    weekend_date = month_start + timedelta(days=weekend_day + 7 * w)
                    if weekend_date.month != month_start.month:
                        continue
                    spend_amount = u_info["weekend_base"] * (
                        1 + self.rng.uniform(-cfg.weekend_noise, cfg.weekend_noise)
                    )
                    spend_amount = round(max(0, spend_amount), 2)
                    spend_dt = datetime(
                        year=weekend_date.year,
                        month=weekend_date.month,
                        day=weekend_date.day,
                        hour=self.rng.randint(10, 22),
                    ) + timedelta(hours=cfg.timezone_offset_hours)

                    weekend_hist.append(spend_amount)
                    records.append(
                        self._make_behavior_row(
                            uid,
                            when=spend_dt,
                            amount=spend_amount,
                            pattern_type="Weekend Spending",
                            pattern_group="weekend",
                            hist=weekend_hist,
                            is_anomaly=False,
                        )
                    )

                    # Possible weekend anomaly
                    if self.rng.random() < cfg.weekend_anomaly_prob:
                        mult = self.rng.uniform(
                            cfg.weekend_anomaly_min_mult,
                            cfg.weekend_anomaly_max_mult,
                        )
                        anomaly_amount = round(spend_amount * mult, 2)
                        anomaly_dt = spend_dt + timedelta(hours=self.rng.randint(0, 6))
                        weekend_hist.append(anomaly_amount)
                        records.append(
                            self._make_behavior_row(
                                uid,
                                when=anomaly_dt,
                                amount=anomaly_amount,
                                pattern_type="Weekend Anomaly",
                                pattern_group="weekend",
                                hist=weekend_hist,
                                is_anomaly=True,
                            )
                        )

        # Sort by timestamp and assign step index
        records.sort(key=lambda r: r["timestamp"])
        for idx, r in enumerate(records, start=1):
            r["step"] = idx

        # Optionally persist per-user profiles to SQLite
        if cfg.persist_profiles:
            self._persist_profiles(users, records)

        return records

    def _make_behavior_row(
        self,
        user_id: str,
        when: datetime,
        amount: float,
        pattern_type: str,
        pattern_group: str,
        hist: List[float],
        is_anomaly: bool,
    ) -> Dict:
        """
        Build a single behavior-driven transaction row with metadata.
        """
        # Historical stats up to THIS transaction
        n = len(hist)
        hist_mean = sum(hist) / n if n else 0.0
        # Simple population std dev
        if n > 1:
            var = sum((x - hist_mean) ** 2 for x in hist) / n
            hist_std = var ** 0.5
        else:
            hist_std = 0.0
        deviation_score = (amount - hist_mean) / (hist_std or 1.0)

        is_fraud = int(is_anomaly or abs(deviation_score) > 3.0)

        duration_sec = self.rng.randint(8, 180) if not is_anomaly else self.rng.choice([1, 2, 3, 4, 5, 300, 450, 600])
        tx_type = "CREDIT" if "Salary" in pattern_type else "DEBIT"
        row: Dict = {
            "user_id": user_id,
            "timestamp": when.isoformat(),
            "hour_of_day": when.hour,
            "day_of_week": when.weekday(),
            "amount": round(amount, 2),
            "type": tx_type,
            "TransactionType": tx_type,
            "pattern_type": pattern_type,
            "pattern_group": pattern_group,
            "hist_mean": round(hist_mean, 2),
            "hist_frequency": len(hist),
            "deviation_score": round(deviation_score, 3),
            "TransactionDuration": duration_sec,
        }

        if self.config.include_balance_fields:
            base_bal = max(amount * 2, self.rng.uniform(50_000, 500_000))
            old_orig = base_bal
            row["oldbalanceOrg"] = round(old_orig, 2)
            row["oldbalanceDest"] = round(self.rng.uniform(0, 50000), 2)
            row["AccountBalance"] = round(old_orig, 2)

        return row

    def _persist_profiles(self, users: List[Dict], records: List[Dict]) -> None:
        """
        Aggregate per-user behavior profiles and write to SQLite.
        """
        cfg = self.config
        by_user: Dict[str, List[Dict]] = {}
        for r in records:
            by_user.setdefault(r["user_id"], []).append(r)

        profiles: List[Dict] = []
        for u in users:
            uid = u["user_id"]
            rows = by_user.get(uid, [])
            if not rows:
                continue
            salary_vals = [r["amount"] for r in rows if r["pattern_group"] == "salary"]
            weekend_vals = [
                r["amount"] for r in rows if r["pattern_group"] == "weekend"
            ]
            def _stats(vals: List[float]) -> Dict[str, float]:
                if not vals:
                    return {"mean": 0.0, "std": 0.0, "count": 0}
                m = sum(vals) / len(vals)
                if len(vals) > 1:
                    var = sum((x - m) ** 2 for x in vals) / len(vals)
                    s = var ** 0.5
                else:
                    s = 0.0
                return {"mean": m, "std": s, "count": len(vals)}

            s = _stats(salary_vals)
            w = _stats(weekend_vals)
            profiles.append(
                {
                    "user_id": uid,
                    "salary_mean": round(s["mean"], 2),
                    "salary_std": round(s["std"], 2),
                    "salary_count": s["count"],
                    "weekend_mean": round(w["mean"], 2),
                    "weekend_std": round(w["std"], 2),
                    "weekend_count": w["count"],
                    "months_simulated": cfg.months,
                }
            )

        if profiles:
            save_to_sqlite(
                profiles,
                db_path=cfg.profiles_db_path,
                table=cfg.profiles_table,
                if_exists="replace",
            )


def create_preset(name: str) -> GeneratorConfig:
    """Return a preset config for common use cases."""
    presets = {
        "education": GeneratorConfig(
            num_transactions=500,
            fraud_ratio=0.05,
            include_balance_fields=True,
            include_timestamps=True,
        ),
        "ml_balanced": GeneratorConfig(
            num_transactions=10000,
            fraud_ratio=0.02,
            seed=42,
            include_balance_fields=True,
            include_timestamps=True,
        ),
        "ml_imbalanced": GeneratorConfig(
            num_transactions=50000,
            fraud_ratio=0.001,
            seed=42,
            include_balance_fields=True,
            include_timestamps=True,
        ),
        "quick_demo": GeneratorConfig(
            num_transactions=100,
            fraud_ratio=0.10,
            include_balance_fields=True,
            include_timestamps=True,
        ),
    }
    return presets.get(name, GeneratorConfig())
