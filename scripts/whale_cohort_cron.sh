#!/bin/bash
# Cron: Sun 03:00 UTC (after poly_data refresh at 02:00)
cd /home/elon-1/workspace/nautilus-trading
flock -n /var/lock/whale-cohort-scanner.lock \
    ./venv/bin/python3 research/whale_cohort_scanner.py \
    >> logs/whale_cohort_scanner.log 2>&1
