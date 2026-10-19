#!/usr/bin/env bash
set -euo pipefail

umask 077
readonly config_file=/run/kvrocks/kvrocks.conf
cp /etc/kvrocks/kvrocks.conf "${config_file}"

# Kvrocks parses quoted configuration values; escape password syntax without
# placing the credential in a process argument or the source ConfigMap.
escaped_password="$(printf '%s' "${REDISCLI_AUTH}" | sed 's/\\/\\\\/g; s/"/\\"/g')"
printf '\nrequirepass "%s"\n' "${escaped_password}" >>"${config_file}"
unset REDISCLI_AUTH escaped_password

exec kvrocks -c "${config_file}"
