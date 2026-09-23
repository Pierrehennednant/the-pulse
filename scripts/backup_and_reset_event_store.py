"""
Back up and reset the V2 event store (/data/geo_event_store.json).

Why: the current store was written by test runs and shadow mode before the
gates A-E / date-sanity fixes landed — about a third of its records have
wrong event_time years, several are duplicate groups, and some are non-Geo
rows the corporate-event policy now rejects. V2 isn't live, so starting it
fresh is cleaner than repairing it record by record.

DO NOT run this without Pierre's go-ahead. Default is a dry run that only
reports what it would do; nothing is written unless --confirm is passed.

    python scripts/backup_and_reset_event_store.py            # dry run
    python scripts/backup_and_reset_event_store.py --confirm  # back up + reset

The backup is verified (re-read and compared to the original) before the
store is reset; if verification fails, the store is left untouched.
"""
import argparse
import json
import os
import shutil
import sys
from datetime import datetime, timezone

DEFAULT_PATH = '/data/geo_event_store.json'


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('--path', default=DEFAULT_PATH, help=f'event store file (default {DEFAULT_PATH})')
    parser.add_argument('--confirm', action='store_true', help='actually write the backup and reset the store')
    args = parser.parse_args()

    if not os.path.exists(args.path):
        print(f"No event store at {args.path} — nothing to back up or reset.")
        return 0

    with open(args.path) as f:
        original = json.load(f)

    stamp = datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')
    root, ext = os.path.splitext(args.path)
    backup_path = f"{root}.backup-{stamp}{ext}"

    print(f"Store:   {args.path} ({len(original)} record(s))")
    print(f"Backup:  {backup_path}")

    if not args.confirm:
        print("DRY RUN — nothing written. Re-run with --confirm to back up and reset.")
        return 0

    shutil.copy2(args.path, backup_path)
    with open(backup_path) as f:
        if json.load(f) != original:
            print("ABORT: backup does not match the original — store left untouched.")
            return 1
    print(f"Backup written and verified ({len(original)} record(s)).")

    tmp_path = f"{args.path}.tmp"
    with open(tmp_path, 'w') as f:
        json.dump({}, f)
    os.replace(tmp_path, args.path)
    print(f"Store reset: {args.path} now holds 0 records.")
    return 0


if __name__ == '__main__':
    sys.exit(main())
