#!/usr/bin/env python3
"""
CLI entrypoint for the synthetic fraud generator.
Used by api.php or for command-line export.
"""

import argparse
import sys
import os

# Ensure package is importable when run from project root
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from generator import SyntheticFraudGenerator, GeneratorConfig, create_preset


def main():
    p = argparse.ArgumentParser(description="Synthetic Fraud Generator CLI")
    p.add_argument("--num", type=int, default=1000, help="Number of transactions")
    p.add_argument("--fraud", type=float, default=0.02, help="Fraud ratio (0-1)")
    p.add_argument("--preset", type=str, default="", help="Preset: education, ml_balanced, ml_imbalanced, quick_demo")
    p.add_argument("--format", choices=["json", "csv"], default="json")
    p.add_argument("--seed", type=int, default=None)
    args = p.parse_args()

    if args.preset:
        config = create_preset(args.preset)
        config.num_transactions = args.num
        config.fraud_ratio = args.fraud
        if args.seed is not None:
            config.seed = args.seed
    else:
        config = GeneratorConfig(
            num_transactions=args.num,
            fraud_ratio=args.fraud,
            seed=args.seed,
        )
    gen = SyntheticFraudGenerator(config)
    data = gen.generate()
    if args.format == "csv":
        print(gen.to_csv(data))
    else:
        import json
        print(json.dumps(data))


if __name__ == "__main__":
    main()
