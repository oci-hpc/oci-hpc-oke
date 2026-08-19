# Copyright (c) 2026 Oracle Corporation and/or its affiliates.
# Licensed under the Universal Permissive License v 1.0 as shown at https://oss.oracle.com/licenses/upl

module "quickcache" {
  count  = alltrue([var.install_quickcache, local.deploy_from_operator]) ? 1 : 0
  source = "./helm-module"

  bastion_host    = module.oke.bastion_public_ip
  bastion_user    = local.bastion_user
  operator_host   = module.oke.operator_private_ip
  operator_user   = local.operator_user
  ssh_private_key = tls_private_key.stack_key.private_key_openssh

  deployment_name = "quickcache"
  namespace       = "kube-system"
  helm_chart_path = "${path.module}/files/oci-quickcache"

  pre_deployment_commands = [
    "export PATH=$PATH:/home/${local.operator_user}/bin",
    "export OCI_CLI_AUTH=instance_principal",
    "export PYTHONWARNINGS=\"ignore:the 'strict' parameter::urllib3.poolmanager\"",
  ]
  deployment_extra_args         = ["--wait", "--timeout", "900s", "--history-max", "2"]
  post_deployment_commands      = []
  helm_template_values_override = local.quickcache_values
  helm_user_values_override     = var.quickcache_values_override

  depends_on = [
    module.oke,
    oci_core_network_security_group_security_rule.quickcache_nfs_ingress,
    oci_core_network_security_group_security_rule.quickcache_nfs_egress,
  ]
}
