# Copyright (c) 2026 Oracle Corporation and/or its affiliates.
# Licensed under the Universal Permissive License v 1.0 as shown at https://oss.oracle.com/licenses/upl

locals {
  quickcache_auto_worker_pools = compact([
    var.worker_gpu_enabled ? "oke-gpu" : "",
    var.worker_rdma_enabled ? "oke-rdma" : "",
    var.worker_gmc_enabled ? "oke-gmc" : "",
  ])
  quickcache_worker_pools_effective = sort(tolist(
    length(var.quickcache_worker_pools) > 0 ? var.quickcache_worker_pools : toset(local.quickcache_auto_worker_pools)
  ))
  quickcache_client_worker_pools_effective = sort(tolist(var.quickcache_client_worker_pools))
  quickcache_available_worker_pools = toset(compact([
    var.worker_cpu_enabled ? "oke-cpu" : "",
    var.worker_gpu_enabled ? "oke-gpu" : "",
    var.worker_rdma_enabled ? "oke-rdma" : "",
    var.worker_gmc_enabled ? "oke-gmc" : "",
  ]))
  quickcache_values = yamlencode({
    controller = {
      virtualShards        = var.quickcache_virtual_shards
      rebalanceMode        = var.quickcache_rebalance_mode
      fallbackGraceSeconds = var.quickcache_rebalance_grace_period
      mapBackupRetention   = var.quickcache_map_backup_retention
    }
    nodeAgent = {
      peerProbeTimeout = var.quickcache_peer_probe_timeout
    }
    clientAgent = {
      enabled = length(local.quickcache_client_worker_pools_effective) > 0
    }
    cache = {
      maxAgeSeconds     = var.quickcache_max_cache_age
      mapReloadInterval = var.quickcache_map_reload_interval
      cacheRangeGets    = var.quickcache_cache_range_gets
      pathLayout        = var.quickcache_cache_path_layout
      access = {
        mode      = var.quickcache_access_mode
        sharedGid = var.quickcache_shared_gid
      }
      cleanup = {
        highWatermark   = var.quickcache_cleanup_high_watermark
        targetWatermark = var.quickcache_cleanup_target_watermark
        maxFiles        = var.quickcache_max_cache_files
      }
    }
  })
}

resource "oci_core_network_security_group_security_rule" "quickcache_nfs_ingress" {
  count = var.install_quickcache ? 1 : 0

  network_security_group_id = module.oke.worker_nsg_id
  direction                 = "INGRESS"
  protocol                  = "6"
  source                    = module.oke.worker_nsg_id
  source_type               = "NETWORK_SECURITY_GROUP"

  tcp_options {
    destination_port_range {
      min = 2049
      max = 2049
    }
  }

  description = "Allow QuickCache NFSv4 between OKE workers"
}

resource "oci_core_network_security_group_security_rule" "quickcache_nfs_egress" {
  count = var.install_quickcache ? 1 : 0

  network_security_group_id = module.oke.worker_nsg_id
  direction                 = "EGRESS"
  protocol                  = "6"
  destination               = module.oke.worker_nsg_id
  destination_type          = "NETWORK_SECURITY_GROUP"

  tcp_options {
    destination_port_range {
      min = 2049
      max = 2049
    }
  }

  description = "Allow QuickCache NFSv4 between OKE workers"
}
