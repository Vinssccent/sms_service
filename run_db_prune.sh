#!/usr/bin/env bash
set -Eeuo pipefail
cd /root/sms_service
# Порог хранения (можешь менять числа):
export PRUNE_SMS_DAYS=30
export PRUNE_ORPHAN_DAYS=14
export PRUNE_SESSION_DAYS=30
# Запуск модуля напрямую из виртуалки (без source)
exec /root/sms_service/venv/bin/python -m seed_scripts.db_maintenance --prune
