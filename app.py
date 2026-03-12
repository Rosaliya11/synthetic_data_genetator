"""
Synthetic Fraud Generator - Web server.
Serves the UI and API for generating privacy-safe fraud datasets.
"""

import os
from flask import Flask, request, jsonify, send_from_directory
from generator import (
    SyntheticFraudGenerator,
    GeneratorConfig,
    create_preset,
    BehavioralConfig,
    BehavioralFraudGenerator,
    save_to_sqlite,
    load_from_sqlite,
    DEFAULT_TABLE,
)

app = Flask(__name__, static_folder="static", static_url_path="")


@app.route("/")
def index():
    return send_from_directory("static", "index.html")


@app.route("/api/presets")
def list_presets():
    return jsonify({
        "education": "Education & awareness (500 tx, 5% fraud)",
        "ml_balanced": "ML training – balanced (10k tx, 2% fraud)",
        "ml_imbalanced": "ML training – imbalanced (50k tx, 0.1% fraud)",
        "quick_demo": "Quick demo (100 tx, 10% fraud)",
    })


@app.route("/api/generate", methods=["POST"])
def generate():
    try:
        body = request.get_json() or {}
        preset_name = body.get("preset")
        if preset_name:
            config = create_preset(preset_name)
            if body.get("num_transactions"):
                config.num_transactions = int(body["num_transactions"])
            if body.get("fraud_ratio") is not None:
                config.fraud_ratio = float(body["fraud_ratio"])
        else:
            config = GeneratorConfig(
                num_transactions=int(body.get("num_transactions", 1000)),
                fraud_ratio=float(body.get("fraud_ratio", 0.02)),
                seed=int(body["seed"]) if body.get("seed") is not None else None,
                include_balance_fields=body.get("include_balance_fields", True),
                include_timestamps=body.get("include_timestamps", True),
                include_realistic_features=body.get("include_realistic_features", True),
                fraud_prefers_transfer=body.get("fraud_prefers_transfer", True),
                fraud_prefers_cash_out=body.get("fraud_prefers_cash_out", True),
                fraud_amount_multiplier=float(body.get("fraud_amount_multiplier", 1.5)),
            )
        gen = SyntheticFraudGenerator(config)
        data = gen.generate()
        format_type = (body.get("format") or "json").lower()
        if format_type == "csv":
            csv_str = gen.to_csv(data)
            return csv_str, 200, {"Content-Type": "text/csv; charset=utf-8"}
        return jsonify(data)
    except Exception as e:
        return jsonify({"error": str(e)}), 400


@app.route("/api/stats", methods=["POST"])
def stats():
    """Return dataset stats without full payload (for preview)."""
    try:
        body = request.get_json() or {}
        config = GeneratorConfig(
            num_transactions=int(body.get("num_transactions", 1000)),
            fraud_ratio=float(body.get("fraud_ratio", 0.02)),
            seed=body.get("seed"),
        )
        gen = SyntheticFraudGenerator(config)
        data = gen.generate()
        total = len(data)
        types = {}
        for r in data:
            t = r.get("type", "?")
            types[t] = types.get(t, 0) + 1
        return jsonify({
            "total_transactions": total,
            "type_breakdown": types,
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 400


@app.route("/api/generate_behavioral", methods=["POST"])
def generate_behavioral():
    """
    Behavior-driven generator endpoint.

    Body parameters (JSON):
      - num_users: int (default 100)
      - months: int (default 18, range 12–24 is typical)
      - seed: int (optional)
      - salary_min, salary_max
      - weekend_min, weekend_max
      - persist_profiles: bool (store per-user stats in SQLite)
      - profiles_db_path, profiles_table
      - save_to_db: bool (optional) + db_path, table, if_exists
      - format: "json" (default) or "csv"
    """
    try:
        body = request.get_json() or {}
        cfg = BehavioralConfig(
            num_users=int(body.get("num_users", 100)),
            months=int(body.get("months", 18)),
            seed=int(body["seed"]) if body.get("seed") is not None else None,
            salary_min=float(body.get("salary_min", 30_000)),
            salary_max=float(body.get("salary_max", 100_000)),
            weekend_min=float(body.get("weekend_min", 5_000)),
            weekend_max=float(body.get("weekend_max", 20_000)),
            persist_profiles=bool(body.get("persist_profiles", False)),
            profiles_db_path=body.get("profiles_db_path", "data/behavior_profiles.db"),
            profiles_table=body.get("profiles_table", "behavior_profiles"),
        )
        gen = BehavioralFraudGenerator(cfg)
        data = gen.generate()

        # Optional transactional DB persistence
        if body.get("save_to_db"):
            db_path = body.get("db_path") or "data/fraud_transactions.db"
            table = body.get("table") or DEFAULT_TABLE
            if_exists = body.get("if_exists") or "append"
            save_to_sqlite(data, db_path=db_path, table=table, if_exists=if_exists)

        fmt = (body.get("format") or "json").lower()
        if fmt == "csv":
            # Reuse existing CSV helper from classic generator
            # by constructing a minimal wrapper
            dummy_cfg = GeneratorConfig()
            dummy_gen = SyntheticFraudGenerator(dummy_cfg)
            csv_str = dummy_gen.to_csv(data)
            return csv_str, 200, {"Content-Type": "text/csv; charset=utf-8"}
        return jsonify(data)
    except Exception as e:
        return jsonify({"error": str(e)}), 400


@app.route("/api/save-to-db", methods=["POST"])
def save_to_db():
    """
    Generate synthetic data and insert it into a SQLite database.
    Body: same as /api/generate, plus:
      - db_path (optional): path to .db file, default "data/fraud_data.db"
      - table (optional): table name, default "transactions"
      - if_exists (optional): "append" or "replace"
    """
    try:
        body = request.get_json() or {}
        db_path = body.get("db_path") or "data/fraud_data.db"
        table = body.get("table") or DEFAULT_TABLE
        if_exists = body.get("if_exists") or "append"

        preset_name = body.get("preset")
        if preset_name:
            config = create_preset(preset_name)
            if body.get("num_transactions"):
                config.num_transactions = int(body["num_transactions"])
            if body.get("fraud_ratio") is not None:
                config.fraud_ratio = float(body["fraud_ratio"])
        else:
            config = GeneratorConfig(
                num_transactions=int(body.get("num_transactions", 1000)),
                fraud_ratio=float(body.get("fraud_ratio", 0.02)),
                seed=int(body["seed"]) if body.get("seed") is not None else None,
                include_balance_fields=body.get("include_balance_fields", True),
                include_timestamps=body.get("include_timestamps", True),
                include_realistic_features=body.get("include_realistic_features", True),
                fraud_prefers_transfer=body.get("fraud_prefers_transfer", True),
                fraud_prefers_cash_out=body.get("fraud_prefers_cash_out", True),
                fraud_amount_multiplier=float(body.get("fraud_amount_multiplier", 1.5)),
            )
        gen = SyntheticFraudGenerator(config)
        data = gen.generate()
        count = save_to_sqlite(data, db_path=db_path, table=table, if_exists=if_exists)
        return jsonify({
            "ok": True,
            "rows_inserted": count,
            "db_path": os.path.abspath(db_path),
            "table": table,
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 400


@app.route("/api/load-from-db", methods=["POST"])
def load_from_db():
    """
    Load rows from an existing SQLite database (preview or export).
    Body: db_path, table (optional), limit (optional).
    """
    try:
        body = request.get_json() or {}
        db_path = body.get("db_path")
        if not db_path:
            return jsonify({"error": "db_path is required"}), 400
        table = body.get("table") or DEFAULT_TABLE
        limit = body.get("limit")
        if limit is not None:
            limit = int(limit)
        rows = load_from_sqlite(db_path, table=table, limit=limit)
        return jsonify({"data": rows, "count": len(rows)})
    except Exception as e:
        return jsonify({"error": str(e)}), 400


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=True)
