#!/usr/bin/env bash
# Pushes .env.production to a Railway service without ever printing a value.
#   scripts/railway-set-vars.sh [service] [env-file]
# Requires: `railway login` and `railway link` done in this directory.
set -euo pipefail

SERVICE="${1:-gum-indexer}"
FILE="${2:-.env.production}"
REQUIRED=(GUM_WEBHOOK__SECRET)

[ -f "$FILE" ] || { echo "missing $FILE" >&2; exit 1; }

declare -a KEYS=() ; declare -a EMPTY=()
while IFS= read -r line || [ -n "$line" ]; do
  [[ "$line" =~ ^[[:space:]]*(#|$) ]] && continue
  key="${line%%=*}"; value="${line#*=}"
  if [ -z "$value" ]; then EMPTY+=("$key"); continue; fi
  case "$key" in
    *__HTTP_URL) [[ "$value" =~ ^https:// ]] || { echo "$key must start with https://" >&2; exit 1; } ;;
    *__WS_URL)   [[ "$value" =~ ^wss://   ]] || { echo "$key must start with wss://" >&2; exit 1; } ;;
  esac
  printf '%s' "$value" | railway variable set "$key" --stdin --service "$SERVICE" --skip-deploys >/dev/null
  KEYS+=("$key")
done < "$FILE"

for r in "${REQUIRED[@]}"; do
  printf '%s\n' "${KEYS[@]}" | grep -qx "$r" || { echo "required variable $r is empty in $FILE" >&2; exit 1; }
done

# A chain without both URLs is deployed disabled instead of failing config validation at boot.
for chain in BASE ARBITRUM MONAD; do
  if printf '%s\n' "${KEYS[@]}" | grep -qx "GUM_CHAINS__${chain}__HTTP_URL" && printf '%s\n' "${KEYS[@]}" | grep -qx "GUM_CHAINS__${chain}__WS_URL"; then
    railway variable set "GUM_CHAINS__${chain}__ENABLED=true" --service "$SERVICE" --skip-deploys >/dev/null
    echo "chain ${chain}: enabled"
  else
    railway variable set "GUM_CHAINS__${chain}__ENABLED=false" --service "$SERVICE" --skip-deploys >/dev/null
    echo "chain ${chain}: DISABLED (URLs missing)"
  fi
done

echo "set ${#KEYS[@]} variables on service '$SERVICE': ${KEYS[*]}"
[ ${#EMPTY[@]} -eq 0 ] || echo "left unset (empty in $FILE): ${EMPTY[*]}"
