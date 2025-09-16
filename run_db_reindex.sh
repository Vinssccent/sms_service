#!/usr/bin/env bash
set -Eeuo pipefail
cd /root/sms_service
exec /root/sms_service/venv/bin/python -m seed_scripts.db_maintenance --reindex
