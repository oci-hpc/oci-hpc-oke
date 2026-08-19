package test

import (
	"testing"

	"github.com/gruntwork-io/terratest/modules/terraform"
	"github.com/stretchr/testify/require"
)

// validationTestCase defines a single validation test scenario
type validationTestCase struct {
	name          string
	vars          map[string]interface{}
	expectedError string
}

// validationTestCases contains all validation scenarios to test
var validationTestCases = []validationTestCase{
	{
		name: "PublicServicesRequirePublicSubnets",
		vars: map[string]interface{}{
			"create_public_subnets":         false,
			"preferred_kubernetes_services": "public",
		},
		expectedError: "Public Kubernetes services require public subnets",
	},
	{
		name: "PublicEndpointRequiresPublicSubnets",
		vars: map[string]interface{}{
			"create_public_subnets":   false,
			"control_plane_is_public": true,
		},
		expectedError: "public cluster endpoint requires public subnets",
	},
	{
		name: "BastionRequiresPublicSubnets",
		vars: map[string]interface{}{
			"create_public_subnets": false,
			"create_bastion":        true,
		},
		expectedError: "Creating a bastion requires public subnets",
	},
	{
		name: "InvalidImageURI",
		vars: map[string]interface{}{
			"worker_ops_image_use_uri":    true,
			"worker_ops_image_custom_uri": "not-a-url",
		},
		expectedError: "Invalid image URI detected",
	},
	{
		name: "InvalidCpuImageURI",
		vars: map[string]interface{}{
			"worker_cpu_image_use_uri":    true,
			"worker_cpu_image_custom_uri": "not-a-url",
		},
		expectedError: "Invalid image URI detected",
	},
	{
		name: "InvalidGpuImageURI",
		vars: map[string]interface{}{
			"worker_gpu_image_use_uri":    true,
			"worker_gpu_image_custom_uri": "not-a-url",
		},
		expectedError: "Invalid image URI detected",
	},
	{
		name: "InvalidRdmaImageURI",
		vars: map[string]interface{}{
			"worker_rdma_image_use_uri":    true,
			"worker_rdma_image_custom_uri": "not-a-url",
		},
		expectedError: "Invalid image URI detected",
	},
	{
		name: "GB200ShapeBlocked",
		vars: map[string]interface{}{
			"worker_rdma_shape": "BM.GPU.GB200.4",
		},
		expectedError: "GB200/GB300 shapes",
	},
	{
		name: "GB200v2ShapeBlocked",
		vars: map[string]interface{}{
			"worker_rdma_shape": "BM.GPU.GB200-v2.4",
		},
		expectedError: "GB200/GB300 shapes",
	},
	{
		name: "GB200v3ShapeBlocked",
		vars: map[string]interface{}{
			"worker_rdma_shape": "BM.GPU.GB200-v3.4",
		},
		expectedError: "GB200/GB300 shapes",
	},
	{
		name: "GB300ShapeBlocked",
		vars: map[string]interface{}{
			"worker_rdma_shape": "BM.GPU.GB300.4",
		},
		expectedError: "GB200/GB300 shapes",
	},
	{
		// Reproduces oracle-quickstart/oci-hpc-oke#97: create_fss=true with no
		// reachable deploy path should fail with a clear precondition error, not
		// hang with an i/o timeout.
		// deploy_to_oke_from_orm=true disables deploy_from_local and deploy_from_operator.
		// current_user_ocid defaults to null so deploy_from_orm is also false,
		// making all three deploy paths inactive and triggering fss_pv_unreachable.
		name: "FSSPVUnreachable",
		vars: map[string]interface{}{
			"create_fss":             true,
			"deploy_to_oke_from_orm": true,
		},
		expectedError: "fss_pv_unreachable",
	},
	{
		name: "PodCapacityExceeded",
		vars: map[string]interface{}{
			// /30 subnet = 4 IPs - 3 reserved = 1 usable
			// default ops pool: 1 node × 31 pods = 31 required > 1 capacity
			"pods_sn_cidr": "10.240.0.0/30",
		},
		expectedError: "Total required pod IPs",
	},
	{
		name: "InvalidSlinkyTopologyBlockSizes",
		vars: map[string]interface{}{
			"slinky_topology_block_sizes": "8,12",
		},
		expectedError: "larger power-of-two",
	},
	{
		name: "QuickCacheRequiresNVMe",
		vars: map[string]interface{}{
			"install_quickcache": true,
			"nvme_raid_enabled":  false,
			"worker_gpu_enabled": true,
		},
		expectedError: "requires nvme_raid_enabled=true",
	},
	{
		name: "QuickCacheCleanupWatermarks",
		vars: map[string]interface{}{
			"install_quickcache":                  true,
			"worker_gpu_enabled":                  true,
			"quickcache_cleanup_target_watermark": 0.95,
			"quickcache_cleanup_high_watermark":   0.90,
		},
		expectedError: "0 < target < high < 1",
	},
	{
		name: "QuickCacheIntegerConfiguration",
		vars: map[string]interface{}{
			"quickcache_max_cache_age": 10.5,
		},
		expectedError: "quickcache_max_cache_age must be a positive integer",
	},
	{
		name: "QuickCacheRebalanceGraceExceedsMapReload",
		vars: map[string]interface{}{
			"install_quickcache":                true,
			"worker_gpu_enabled":                true,
			"quickcache_rebalance_mode":         "automatic",
			"quickcache_map_reload_interval":    60,
			"quickcache_rebalance_grace_period": 60,
		},
		expectedError: "grace period must be greater than the shard-map reload interval",
	},
	{
		name: "QuickCacheStagedShardCountLimit",
		vars: map[string]interface{}{
			"install_quickcache":        true,
			"worker_gpu_enabled":        true,
			"quickcache_rebalance_mode": "automatic",
			"quickcache_virtual_shards": 8192,
		},
		expectedError: "support at most 4096 virtual shards",
	},
	{
		name: "QuickCacheServerClientPoolOverlap",
		vars: map[string]interface{}{
			"install_quickcache":             true,
			"worker_gpu_enabled":             true,
			"quickcache_worker_pools":        []string{"oke-gpu"},
			"quickcache_client_worker_pools": []string{"oke-gpu"},
		},
		expectedError: "server and client worker pools must not overlap",
	},
	{
		name: "QuickCacheRequiresDeploymentPath",
		vars: map[string]interface{}{
			"install_quickcache":      true,
			"worker_gpu_enabled":      true,
			"control_plane_is_public": false,
			"deploy_to_oke_from_orm":  false,
			"create_bastion":          false,
			"create_operator":         false,
		},
		expectedError: "requires a reachable OKE deployment path",
	},
}

// TestValidation runs all validation test cases in parallel using table-driven tests
func TestValidation(t *testing.T) {
	t.Parallel()

	for _, tc := range validationTestCases {
		tc := tc // capture range variable for parallel execution
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			// Each parallel subtest gets its own copy of the terraform directory
			// to prevent concurrent terraform init calls from racing on lock files.
			tmpDir := copyTerraformToTemp(t)
			options := newTerraformOptions(t, tc.vars)
			options.TerraformDir = tmpDir
			assertPlanFailsWithError(t, options, tc.expectedError)
		})
	}
}

func assertPlanFailsWithError(t *testing.T, options *terraform.Options, expected string) {
	t.Helper()
	// Use zero retries so the exact terraform error output is returned directly.
	// Retries wrap errors in MaxRetriesExceeded/FatalError, which drops the
	// original terraform plan output and breaks require.Contains.
	noRetry := *options
	noRetry.MaxRetries = 0
	noRetry.RetryableTerraformErrors = nil
	_, err := terraform.InitAndPlanE(t, &noRetry)
	require.Error(t, err)
	require.Contains(t, err.Error(), expected)
}
