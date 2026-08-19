# Copyright (c) 2026 Oracle Corporation and/or its affiliates.
# Licensed under the Universal Permissive License v 1.0 as shown at https://oss.oracle.com/licenses/upl

resource "helm_release" "quickcache" {
  count = alltrue([var.install_quickcache, local.deploy_from_local || local.deploy_from_orm]) ? 1 : 0

  depends_on = [
    module.oke,
    data.oci_resourcemanager_private_endpoint_reachable_ip.oke,
    oci_core_network_security_group_security_rule.quickcache_nfs_ingress,
    oci_core_network_security_group_security_rule.quickcache_nfs_egress,
  ]

  namespace        = "kube-system"
  name             = "quickcache"
  chart            = "${path.module}/files/oci-quickcache"
  create_namespace = false
  wait             = true
  timeout          = 900
  max_history      = 2
  values           = compact([local.quickcache_values, trimspace(var.quickcache_values_override)])
}
