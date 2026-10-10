#!/usr/bin/env bash
set -euo pipefail

exec 3<>/dev/tcp/127.0.0.1/9083
exec 3>&-
exec 3<&-
