#!/usr/bin/env bash
set -euo pipefail

mount_root=${HOST_MOUNT_ROOT:?}
node_mode=${NODE_MODE:-server}
mount_root=$(realpath -m -- "${mount_root}")
case "${mount_root}" in
  /var/lib/ociqc/*) ;;
  *)
    echo "Refusing to tear down an unsafe QuickCache mount root: ${mount_root}" >&2
    exit 1
    ;;
esac

shopt -s nullglob
for peer_mount in "${mount_root}"/*; do
  if mountpoint -q "${peer_mount}"; then
    umount -l "${peer_mount}" || true
  fi
  rmdir "${peer_mount}" 2>/dev/null || true
done

rm -f /var/lib/ociqc/host-ready
if [[ ${node_mode} == server ]]; then
  rm -f /etc/exports.d/oci-quickcache.exports
  if command -v exportfs >/dev/null 2>&1; then
    exportfs -ra
  fi
fi
