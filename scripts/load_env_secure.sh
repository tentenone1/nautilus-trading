#!/bin/bash
# Decrypt .env.gpg and export all variables into current shell
# Usage: source scripts/load_env_secure.sh
# Or: eval $(scripts/load_env_secure.sh)

if [ ! -f /home/elon-1/workspace/nautilus-trading/.env.gpg ]; then
    echo 'ERROR: .env.gpg not found' >&2
    exit 1
fi

# Decrypt to a temp file, source it, then delete
ENV_TEMP=$(mktemp /tmp/nautilus_env.XXXXXX)
chmod 600 "$ENV_TEMP"
gpg --batch --yes --decrypt /home/elon-1/workspace/nautilus-trading/.env.gpg > "$ENV_TEMP" 2>/dev/null

# Export all KEY=VALUE lines
while IFS='=' read -r key value; do
    # Skip comments and empty lines
    [[ "$key" =~ ^#.*$ ]] && continue
    [[ -z "$key" ]] && continue
    export "$key"="$value"
done < "$ENV_TEMP"

# Wipe temp file securely
shred -u "$ENV_TEMP" 2>/dev/null || rm -f "$ENV_TEMP"
