#!/usr/bin/env bash
set -euo pipefail

local_cache_path=${LOCAL_CACHE_PATH:?}
mount_root=${HOST_MOUNT_ROOT:?}
runtime_root=${HOST_RUNTIME_ROOT:?}
config_root=${HOST_CONFIG_ROOT:?}
log_root=${HOST_LOG_ROOT:?}
cache_dir_name=${CACHE_DIR_NAME:?}
access_mode=${ACCESS_MODE:?}
shared_gid=${SHARED_GID:?}
node_mode=${NODE_MODE:-server}
node_uid=${NODE_UID:?}

local_cache_path=$(realpath -m -- "${local_cache_path}")
mount_root=$(realpath -m -- "${mount_root}")
runtime_root=$(realpath -m -- "${runtime_root}")
config_root=$(realpath -m -- "${config_root}")
log_root=$(realpath -m -- "${log_root}")
if [[ ${node_mode} == server ]]; then
  case "${local_cache_path}" in
    /mnt/nvme/*) ;;
    *)
      echo "QuickCache local cache path must remain below /mnt/nvme" >&2
      exit 1
      ;;
  esac
elif [[ ${node_mode} != client ]]; then
  echo "QuickCache NODE_MODE must be server or client" >&2
  exit 1
fi
for host_path in "${mount_root}" "${runtime_root}" "${config_root}" "${log_root}"; do
  case "${host_path}" in
    /var/lib/ociqc/*) ;;
    *)
      echo "QuickCache host paths must remain below /var/lib/ociqc" >&2
      exit 1
      ;;
  esac
done
host_paths=("${mount_root}" "${runtime_root}" "${config_root}" "${log_root}")
for ((first = 0; first < ${#host_paths[@]}; first++)); do
  for ((second = first + 1; second < ${#host_paths[@]}; second++)); do
    first_path=${host_paths[first]}
    second_path=${host_paths[second]}
    if [[ ${first_path} == "${second_path}" || ${first_path} == "${second_path}"/* || ${second_path} == "${first_path}"/* ]]; then
      echo "QuickCache host mount, runtime, and config paths must not overlap" >&2
      exit 1
    fi
  done
done
if [[ ! ${cache_dir_name} =~ ^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$ ]]; then
  echo "QuickCache cache directory name must be one safe path component" >&2
  exit 1
fi
if [[ ${access_mode} != trustedShared && ${access_mode} != sharedGroup ]]; then
  echo "QuickCache ACCESS_MODE must be trustedShared or sharedGroup" >&2
  exit 1
fi
if [[ ! ${shared_gid} =~ ^[1-9][0-9]*$ ]] || ((shared_gid > 2147483647)); then
  echo "QuickCache SHARED_GID must be a valid positive Linux group ID" >&2
  exit 1
fi

install_debian_packages() {
  local packages=("$@")
  local attempt
  local delay=5
  local max_attempts=5

  for ((attempt = 1; attempt <= max_attempts; attempt++)); do
    echo "Installing QuickCache host packages (attempt ${attempt}/${max_attempts}): ${packages[*]}" >&2
    if DEBIAN_FRONTEND=noninteractive apt-get \
      -o DPkg::Lock::Timeout=120 \
      install -y --no-install-recommends "${packages[@]}"; then
      return 0
    fi

    echo "Host package installation failed; refreshing package indexes before retry" >&2
    DEBIAN_FRONTEND=noninteractive apt-get \
      -o DPkg::Lock::Timeout=120 \
      update || echo "Package index refresh failed; retrying with existing indexes" >&2

    if ((attempt < max_attempts)); then
      sleep "${delay}"
      delay=$((delay * 2))
    fi
  done

  echo "Unable to install QuickCache host packages after ${max_attempts} attempts" >&2
  return 1
}

if [[ ${node_mode} == server ]] && ! mountpoint -q /mnt/nvme; then
  echo "QuickCache requires the existing OKE NVMe filesystem at /mnt/nvme" >&2
  exit 1
fi

if [[ ${node_mode} == server ]]; then
  nvme_device=$(findmnt -n -o SOURCE /mnt/nvme)
  root_device_id=$(findmnt -n -o MAJ:MIN /)
  nvme_device_id=$(findmnt -n -o MAJ:MIN /mnt/nvme)
  if [[ -z ${nvme_device} || -z ${nvme_device_id} || ${root_device_id} == "${nvme_device_id}" ]]; then
    echo "Refusing to use the root filesystem for QuickCache" >&2
    exit 1
  fi

  mkdir -p "${local_cache_path}"
  cache_device_id=$(findmnt -n -o MAJ:MIN --target "${local_cache_path}")
  if [[ -z ${cache_device_id} || ${cache_device_id} != "${nvme_device_id}" ]]; then
    echo "QuickCache local cache path is not backed by the /mnt/nvme filesystem" >&2
    exit 1
  fi
fi

if ! command -v mount.nfs4 >/dev/null 2>&1 || { [[ ${node_mode} == server ]] && ! command -v exportfs >/dev/null 2>&1; }; then
  if [[ -f /etc/os-release ]]; then
    # shellcheck disable=SC1091
    . /etc/os-release
  fi
  case "${ID:-}" in
    ubuntu|debian)
      nfs_packages=(nfs-common)
      if [[ ${node_mode} == server ]]; then
        nfs_packages+=(nfs-kernel-server)
      fi
      install_debian_packages "${nfs_packages[@]}"
      ;;
    ol|rhel|rocky|almalinux)
      dnf install -y nfs-utils || yum install -y nfs-utils
      ;;
    *)
      echo "Unsupported host OS for automatic NFS setup: ${ID:-unknown}" >&2
      exit 1
      ;;
  esac
fi

if [[ ${node_mode} == server ]] && { ! command -v python3 >/dev/null 2>&1 || ! command -v setpriv >/dev/null 2>&1; }; then
  if [[ -f /etc/os-release ]]; then
    # shellcheck disable=SC1091
    . /etc/os-release
  fi
  case "${ID:-}" in
    ubuntu|debian)
      install_debian_packages python3 util-linux
      ;;
    ol|rhel|rocky|almalinux)
      dnf install -y python3 util-linux || yum install -y python3 util-linux
      ;;
    *)
      echo "Unsupported host OS for QuickCache migration setup: ${ID:-unknown}" >&2
      exit 1
      ;;
  esac
fi

if ! command -v mount.nfs4 >/dev/null 2>&1 || { [[ ${node_mode} == server ]] && ! command -v exportfs >/dev/null 2>&1; }; then
  echo "QuickCache NFS prerequisites are still unavailable after package setup" >&2
  exit 1
fi
if [[ ${node_mode} == server ]] && { ! command -v python3 >/dev/null 2>&1 || ! command -v setpriv >/dev/null 2>&1; }; then
  echo "QuickCache migration prerequisites are unavailable after package setup" >&2
  exit 1
fi

mkdir -p "${mount_root}" "${runtime_root}" "${config_root}" "${log_root}"
if [[ ${node_mode} == server ]]; then
  mkdir -p "${local_cache_path}/${cache_dir_name}"
fi
if [[ ${access_mode} == sharedGroup ]]; then
  chgrp "${shared_gid}" "${log_root}"
  chmod 3770 "${log_root}"
  if [[ ${node_mode} == server ]]; then
    chgrp "${shared_gid}" "${local_cache_path}/${cache_dir_name}"
    chmod 3770 "${local_cache_path}/${cache_dir_name}"
  fi
  log_file_mode=0660
else
  chmod 1777 "${log_root}"
  if [[ ${node_mode} == server ]]; then
    chmod 1777 "${local_cache_path}/${cache_dir_name}"
  fi
  log_file_mode=0666
fi
touch "${log_root}/cache_log.csv" "${log_root}/cache_err.csv" \
  "${log_root}/cleanup.log" "${log_root}/shard_map_audit.log"
if [[ ${access_mode} == sharedGroup ]]; then
  chgrp "${shared_gid}" "${log_root}/cache_log.csv" "${log_root}/cache_err.csv" \
    "${log_root}/cleanup.log" "${log_root}/shard_map_audit.log"
fi
chmod "${log_file_mode}" "${log_root}/cache_log.csv" "${log_root}/cache_err.csv" \
  "${log_root}/cleanup.log" "${log_root}/shard_map_audit.log"

cat > /etc/logrotate.d/oci-quickcache <<EOF
${log_root}/*.csv ${log_root}/*.log {
    daily
    rotate 7
    compress
    missingok
    notifempty
    copytruncate
}
EOF

if [[ ${node_mode} == server ]]; then
  mkdir -p /etc/exports.d
  exports_file=/etc/exports.d/oci-quickcache.exports
  desired_export="${local_cache_path} *(rw,async,no_subtree_check,root_squash,fsid=0)"
  if [[ ! -f ${exports_file} ]] || [[ $(<"${exports_file}") != "${desired_export}" ]]; then
    printf '%s\n' "${desired_export}" > "${exports_file}"
  fi

  systemctl daemon-reload
  if systemctl cat nfs-kernel-server.service >/dev/null 2>&1; then
    systemctl enable --now nfs-kernel-server
  elif systemctl cat nfs-server.service >/dev/null 2>&1; then
    systemctl enable --now nfs-server
  else
    echo "QuickCache could not find an NFS server systemd unit after package setup" >&2
    exit 1
  fi
  exportfs -ra

  self_mount="${mount_root}/${node_uid}"
  mkdir -p "${self_mount}"
  if ! mountpoint -q "${self_mount}"; then
    mount --bind "${local_cache_path}" "${self_mount}"
  fi
fi

touch /var/lib/ociqc/host-ready
