#!/usr/bin/env bash

set -Eeuo pipefail

script_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
project_dir=$(cd -- "$script_dir/.." && pwd)
archive_dir="$project_dir/data/request-log-archives"
keep_original=0
dry_run=0
disable_check=0
current_partial=""
source_manifest=""
destination_manifest=""

usage() {
    cat <<'EOF'
Usage: recompress_request_log_archives.sh [options]

Convert request_logs_*.tar.gz archives to tar.xz one at a time. The source
tar.gz is deleted only after the new archive passes XZ and manifest checks.

Options:
  --archive-dir DIR  Archive directory (default: project data directory)
  --keep-original   Keep tar.gz files after successful conversion
  --disable-check   Only decompress and recompress; skip archive verification
  --dry-run         Show files that would be converted
  -h, --help        Show this help
EOF
}

log() {
    printf '[%(%Y-%m-%dT%H:%M:%S%z)T] %s\n' -1 "$*"
}

fail() {
    log "ERROR: $*" >&2
    exit 1
}

cleanup() {
    if [[ -n "$current_partial" && -e "$current_partial" ]]; then
        rm -f -- "$current_partial"
    fi
    if [[ -n "$source_manifest" && -e "$source_manifest" ]]; then
        rm -f -- "$source_manifest"
    fi
    if [[ -n "$destination_manifest" && -e "$destination_manifest" ]]; then
        rm -f -- "$destination_manifest"
    fi
}
trap cleanup EXIT INT TERM

while (($#)); do
    case "$1" in
        --archive-dir)
            (($# >= 2)) || fail "--archive-dir requires a value"
            archive_dir=$2
            shift 2
            ;;
        --keep-original)
            keep_original=1
            shift
            ;;
        --disable-check)
            disable_check=1
            shift
            ;;
        --dry-run)
            dry_run=1
            shift
            ;;
        -h|--help)
            usage
            exit 0
            ;;
        *)
            fail "unknown option: $1"
            ;;
    esac
done

for command_name in flock gzip xz tar cmp df stat; do
    command -v "$command_name" >/dev/null 2>&1 || fail "required command not found: $command_name"
done

[[ -d "$archive_dir" ]] || fail "archive directory not found: $archive_dir"
archive_dir=$(cd -- "$archive_dir" && pwd)

exec 9>"$archive_dir/.recompress.lock"
flock -n 9 || fail "another archive conversion is already running"

shopt -s nullglob
archives=("$archive_dir"/request_logs_*.tar.gz)
if ((${#archives[@]} == 0)); then
    log "no tar.gz archives found in $archive_dir"
    exit 0
fi

log "found ${#archives[@]} tar.gz archive(s) in $archive_dir"
if ((disable_check)); then
    log "archive verification is disabled"
fi

converted=0
removed=0
skipped=0

for source_path in "${archives[@]}"; do
    archive_base=$(basename -- "${source_path%.tar.gz}")
    manifest_name="$archive_base.manifest.json"
    destination_path="${source_path%.tar.gz}.tar.xz"
    current_partial="$destination_path.partial"

    if ((dry_run)); then
        log "would convert $(basename -- "$source_path") -> $(basename -- "$destination_path")"
        ((skipped += 1))
        current_partial=""
        continue
    fi

    source_bytes=$(stat -c %s -- "$source_path")
    available_bytes=$(df -PB1 -- "$archive_dir" | awk 'NR == 2 {print $4}')
    ((available_bytes > source_bytes)) || fail "insufficient free space for $(basename -- "$source_path")"

    if ((disable_check == 0)); then
        source_manifest=$(mktemp "$archive_dir/.source-manifest.XXXXXX")
        destination_manifest=$(mktemp "$archive_dir/.destination-manifest.XXXXXX")
        tar --occurrence=1 -xOzf "$source_path" "$manifest_name" >"$source_manifest"
    fi

    if [[ -e "$destination_path" ]]; then
        if ((disable_check)); then
            fail "destination exists while checks are disabled: $destination_path"
        fi
        log "validating existing $(basename -- "$destination_path")"
        env -u XZ_DEFAULTS -u XZ_OPT xz -t -- "$destination_path"
        tar --occurrence=1 -xOJf "$destination_path" "$manifest_name" >"$destination_manifest"
        cmp -s -- "$source_manifest" "$destination_manifest" \
            || fail "manifest mismatch: $destination_path"
        if ((keep_original == 0)); then
            rm -- "$source_path"
            ((removed += 1))
            log "removed verified source $(basename -- "$source_path")"
        else
            log "kept source $(basename -- "$source_path")"
        fi
        rm -f -- "$source_manifest" "$destination_manifest"
        source_manifest=""
        destination_manifest=""
        ((skipped += 1))
        current_partial=""
        continue
    fi

    rm -f -- "$current_partial"
    log "converting $(basename -- "$source_path") -> $(basename -- "$destination_path")"
    gzip -dc -- "$source_path" \
        | env -u XZ_DEFAULTS -u XZ_OPT xz -T2 --lzma2=preset=9e,dict=1536MiB,lc=4,lp=0,pb=0 -c \
        >"$current_partial"

    chmod 0640 "$current_partial"
    if ((disable_check == 0)); then
        env -u XZ_DEFAULTS -u XZ_OPT xz -t -- "$current_partial"
        tar --occurrence=1 -xOJf "$current_partial" "$manifest_name" >"$destination_manifest"
        cmp -s -- "$source_manifest" "$destination_manifest" \
            || fail "manifest mismatch after conversion: $source_path"
    fi

    mv -- "$current_partial" "$destination_path"
    current_partial=""
    rm -f -- "$source_manifest" "$destination_manifest"
    source_manifest=""
    destination_manifest=""
    ((converted += 1))

    if ((keep_original == 0)); then
        rm -- "$source_path"
        ((removed += 1))
    fi
    log "completed $(basename -- "$destination_path")"
done

log "done: converted=$converted removed_sources=$removed skipped=$skipped"
