package test

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestQuickCacheNeverFormatsStorage(t *testing.T) {
	hostSetup := readRepositoryFile(t, "terraform", "files", "oci-quickcache", "files", "host_setup.sh")
	nodeAgent := readRepositoryFile(t, "terraform", "files", "oci-quickcache", "files", "node_agent.py")
	contents := hostSetup + nodeAgent

	for _, destructive := range []string{"mkfs", "mdadm --create", "pvcreate", "vgcreate", "lvcreate", "wipefs"} {
		require.NotContains(t, contents, destructive)
	}
	require.Contains(t, hostSetup, "mountpoint -q /mnt/nvme")
	require.Contains(t, hostSetup, "Refusing to use the root filesystem")
}

func TestQuickCacheRuntimeDoesNotUseRawObjectKeysAsPaths(t *testing.T) {
	sitecustomize := readRepositoryFile(t, "terraform", "files", "oci-quickcache", "files", "sitecustomize.py")
	cachePaths := readRepositoryFile(t, "terraform", "files", "oci-quickcache", "files", "cache_paths.py")

	require.Contains(t, cachePaths, "resource_hash")
	require.Contains(t, cachePaths, "digest[:2]")
	require.NotContains(t, cachePaths, "os.path.dirname(key)")
	require.Contains(t, sitecustomize, "cache_path(")
}

func TestQuickCacheUsesControllerOwnedDynamicState(t *testing.T) {
	controller := readRepositoryFile(t, "terraform", "files", "oci-quickcache", "files", "controller.py")
	chartTemplates := readRepositoryFile(t, "terraform", "files", "oci-quickcache", "templates", "runtime-configmap.yaml")

	require.Contains(t, controller, "create_namespaced_config_map")
	require.Contains(t, controller, "replace_namespaced_config_map")
	require.False(t, strings.Contains(chartTemplates, "shard_map.json"), "Helm must not overwrite the live shard map")
}

func TestQuickCacheStagedRebalanceCopiesBeforeCutover(t *testing.T) {
	controller := readRepositoryFile(t, "terraform", "files", "oci-quickcache", "files", "controller.py")
	migrator := readRepositoryFile(t, "terraform", "files", "oci-quickcache", "files", "migrate_shards.py")
	sitecustomize := readRepositoryFile(t, "terraform", "files", "oci-quickcache", "files", "sitecustomize.py")
	nodeAgentTemplate := readRepositoryFile(t, "terraform", "files", "oci-quickcache", "templates", "node-agent.yaml")

	require.Contains(t, controller, `plan["phase"] = "migrating"`)
	require.Contains(t, controller, `state["previous"] = active`)
	require.Contains(t, controller, `state["active"] = state["pending"]`)
	require.Contains(t, migrator, `if phase == "cleanup"`)
	require.Contains(t, migrator, `_coverage_complete`)
	require.Contains(t, sitecustomize, `HIT_PREVIOUS`)
	require.Contains(t, nodeAgentTemplate, `- name: migration-agent`)
}

func TestQuickCacheRetainsFullMapBackupsAndEstimatesManualMigration(t *testing.T) {
	controller := readRepositoryFile(t, "terraform", "files", "oci-quickcache", "files", "controller.py")
	migrator := readRepositoryFile(t, "terraform", "files", "oci-quickcache", "files", "migrate_shards.py")
	migrationAgent := readRepositoryFile(t, "terraform", "files", "oci-quickcache", "files", "migration_agent.py")
	role := readRepositoryFile(t, "terraform", "files", "oci-quickcache", "templates", "role.yaml")
	benchmarkRunner := readRepositoryFile(t, "manifests", "quickcache", "run-multinode-benchmark.sh")
	benchmarkReport := readRepositoryFile(t, "manifests", "quickcache", "benchmark-report.py")
	benchmarkKeys := readRepositoryFile(t, "manifests", "quickcache", "prepare-benchmark-keys.py")

	require.Contains(t, controller, "_backup_shard_map(")
	require.Contains(t, controller, `f"shard_map.{timestamp}.bak"`)
	require.Contains(t, controller, "MAP_BACKUP_RETENTION")
	require.Contains(t, role, `"delete"`)
	require.Contains(t, migrator, `if phase == "estimate"`)
	require.Contains(t, migrationAgent, `if phase_name == "awaitingApproval"`)
	require.Contains(t, controller, `plan["estimate"] = estimate`)
	require.Contains(t, benchmarkRunner, "QC_BASELINE_AGGREGATE_GIB_S")
	require.Contains(t, benchmarkReport, `checks["minimum_baseline_parity"]`)
	require.Contains(t, benchmarkReport, `checks["expected_cache_owner_coverage"]`)
	require.Contains(t, benchmarkKeys, "select_keys(")
}

func TestQuickCacheResourceManagerWorkerPoolsAreSelectable(t *testing.T) {
	schema := readRepositoryFile(t, "terraform", "schema.yaml")
	start := strings.Index(schema, "  quickcache_worker_pools:")
	require.NotEqual(t, -1, start)
	endOffset := strings.Index(schema[start:], "\n  quickcache_virtual_shards:")
	require.NotEqual(t, -1, endOffset)
	workerPools := schema[start : start+endOffset]

	require.Contains(t, workerPools, "type: enum")
	require.Contains(t, workerPools, "allowMultiple: true")
	for _, pool := range []string{"oke-gpu", "oke-rdma", "oke-gmc", "oke-cpu"} {
		require.Contains(t, workerPools, "- "+pool)
	}
}

func TestQuickCacheRuntimeHandlesHelmNumericFormatting(t *testing.T) {
	nodeAgentTemplate := readRepositoryFile(t, "terraform", "files", "oci-quickcache", "templates", "node-agent.yaml")
	cleanup := readRepositoryFile(t, "terraform", "files", "oci-quickcache", "files", "cleanup.py")
	nodeAgent := readRepositoryFile(t, "terraform", "files", "oci-quickcache", "files", "node_agent.py")

	require.Contains(t, nodeAgentTemplate, `printf "%d" (int64 .Values.cache.cleanup.maxFiles)`)
	require.Contains(t, nodeAgentTemplate, `printf "%d" (int64 .Values.cache.maxAgeSeconds)`)
	require.Contains(t, cleanup, "Decimal(value)")
	require.Contains(t, cleanup, "number.to_integral_value()")
	require.Contains(t, nodeAgent, `_parse_positive_integer(`)
	require.Contains(t, nodeAgent, `os.environ["CACHE_MAX_AGE"]`)
}

func TestQuickCacheHostSetupRetriesAndReportsFailures(t *testing.T) {
	hostSetup := readRepositoryFile(t, "terraform", "files", "oci-quickcache", "files", "host_setup.sh")
	nodeAgent := readRepositoryFile(t, "terraform", "files", "oci-quickcache", "files", "node_agent.py")

	require.Contains(t, hostSetup, "DPkg::Lock::Timeout=120")
	require.Contains(t, hostSetup, "max_attempts=5")
	require.Contains(t, hostSetup, "QuickCache NFS prerequisites are still unavailable")
	require.Contains(t, nodeAgent, "host command failed: exit_code=%s")
	require.Contains(t, nodeAgent, "stderr (tail):")
	require.Contains(t, nodeAgent, "HEALTH_FILE.touch()")
}

func TestQuickCacheEntersTheActualHostRoot(t *testing.T) {
	nodeAgent := readRepositoryFile(t, "terraform", "files", "oci-quickcache", "files", "node_agent.py")

	require.Contains(t, nodeAgent, `os.open("/proc/1/root", os.O_RDONLY | os.O_DIRECTORY)`)
	require.NotContains(t, nodeAgent, `host_root_fd = os.open("/host"`)
}

func TestQuickCachePublishesWorkloadMountPaths(t *testing.T) {
	nodeAgent := readRepositoryFile(t, "terraform", "files", "oci-quickcache", "files", "node_agent.py")
	sharding := readRepositoryFile(t, "terraform", "files", "oci-quickcache", "files", "sharding.py")

	require.Contains(t, nodeAgent, "resolve_shard_mounts(shard_map, peers)")
	require.Contains(t, nodeAgent, `f"{config_root}/shard_map.json", workload_shard_map`)
	require.Contains(t, sharding, `os.path.isabs(mount_path)`)
}

func TestQuickCacheHelmWaitsForDataPlaneReadiness(t *testing.T) {
	readinessJob := readRepositoryFile(t, "terraform", "files", "oci-quickcache", "templates", "readiness-job.yaml")
	runtimeConfig := readRepositoryFile(t, "terraform", "files", "oci-quickcache", "templates", "runtime-configmap.yaml")
	role := readRepositoryFile(t, "terraform", "files", "oci-quickcache", "templates", "role.yaml")

	require.Contains(t, readinessJob, `"helm.sh/hook": post-install,post-upgrade`)
	require.Contains(t, readinessJob, `command: ["uv", "run", "/runtime/healthcheck.py"]`)
	require.Contains(t, runtimeConfig, `healthcheck.py:`)
	require.Contains(t, role, `resources: ["deployments", "daemonsets"]`)
}

func TestQuickCacheNamespacedRBACIsNotClusterWide(t *testing.T) {
	clusterRole := readRepositoryFile(t, "terraform", "files", "oci-quickcache", "templates", "clusterrole.yaml")
	role := readRepositoryFile(t, "terraform", "files", "oci-quickcache", "templates", "role.yaml")

	require.Contains(t, clusterRole, `resources: ["nodes"]`)
	require.NotContains(t, clusterRole, `resources: ["configmaps"]`)
	require.NotContains(t, clusterRole, `resources: ["deployments", "daemonsets"]`)
	require.Contains(t, role, `resources: ["configmaps"]`)
	require.Contains(t, role, `resources: ["deployments", "daemonsets"]`)
}

func TestQuickCacheUsesExplicitLRUAndBestEffortWrites(t *testing.T) {
	sitecustomize := readRepositoryFile(t, "terraform", "files", "oci-quickcache", "files", "sitecustomize.py")

	require.Contains(t, sitecustomize, "_touch_access_time(path)")
	require.Contains(t, sitecustomize, `Path(f"{path}.access")`)
	require.Contains(t, sitecustomize, "incomplete cache body:")
	require.Contains(t, sitecustomize, "MISS_NO_CACHE:")
	require.Contains(t, sitecustomize, "directory.chmod(CACHE_DIRECTORY_MODE)")
}

func TestQuickCacheOperationalParityFeatures(t *testing.T) {
	nodeAgent := readRepositoryFile(t, "terraform", "files", "oci-quickcache", "files", "node_agent.py")
	hostSetup := readRepositoryFile(t, "terraform", "files", "oci-quickcache", "files", "host_setup.sh")
	sitecustomize := readRepositoryFile(t, "terraform", "files", "oci-quickcache", "files", "sitecustomize.py")
	clientAgent := readRepositoryFile(t, "terraform", "files", "oci-quickcache", "templates", "client-agent.yaml")
	benchmark := readRepositoryFile(t, "manifests", "quickcache", "multinode-benchmark-job.yaml")

	require.Contains(t, nodeAgent, "_probe_peer(mount_path)")
	require.Contains(t, nodeAgent, `"NODE_MODE", "server"`)
	require.Contains(t, hostSetup, "/etc/logrotate.d/oci-quickcache")
	require.Contains(t, nodeAgent, "shard_map_audit.log")
	require.Contains(t, hostSetup, "sharedGroup")
	require.Contains(t, sitecustomize, "fcntl.flock")
	require.Contains(t, clientAgent, "quickcacheClientLabel")
	require.Contains(t, clientAgent, "value: client")
	require.Contains(t, benchmark, "throughput_mib_s")
	require.Contains(t, benchmark, "topologySpreadConstraints")
}

func TestQuickCacheRejectsRootFilesystemBindMounts(t *testing.T) {
	hostSetup := readRepositoryFile(t, "terraform", "files", "oci-quickcache", "files", "host_setup.sh")

	require.Contains(t, hostSetup, "findmnt -n -o MAJ:MIN /")
	require.Contains(t, hostSetup, "root_device_id")
	require.Contains(t, hostSetup, "nvme_device_id")
}

func TestQuickCacheRejectsUnsafeOverridePaths(t *testing.T) {
	nodeAgent := readRepositoryFile(t, "terraform", "files", "oci-quickcache", "files", "node_agent.py")
	compactNodeAgent := strings.Join(strings.Fields(nodeAgent), " ")
	hostSetup := readRepositoryFile(t, "terraform", "files", "oci-quickcache", "files", "host_setup.sh")
	hostTeardown := readRepositoryFile(t, "terraform", "files", "oci-quickcache", "files", "host_teardown.sh")
	validation := readRepositoryFile(t, "terraform", "files", "oci-quickcache", "templates", "validate.yaml")

	require.Contains(t, compactNodeAgent, `_validated_child_path( os.environ["LOCAL_CACHE_PATH"], "/mnt/nvme", "LOCAL_CACHE_PATH" )`)
	require.Contains(t, compactNodeAgent, `_validated_host_child_path(os.environ[name], "/var/lib/ociqc", name)`)
	require.Contains(t, nodeAgent, `_reject_overlapping_paths(host_paths)`)
	require.Contains(t, hostSetup, "QuickCache local cache path must remain below /mnt/nvme")
	require.Contains(t, hostSetup, "must not overlap")
	require.Contains(t, hostTeardown, "Refusing to tear down an unsafe QuickCache mount root")
	require.Contains(t, validation, "cache.cacheDirName must be one safe path component")
	require.Contains(t, validation, "must not overlap")
}

func TestQuickCacheCleanupDoesNotMountTheHostRoot(t *testing.T) {
	nodeAgentTemplate := readRepositoryFile(t, "terraform", "files", "oci-quickcache", "templates", "node-agent.yaml")

	cleanupStart := strings.Index(nodeAgentTemplate, "        - name: cleanup")
	require.NotEqual(t, -1, cleanupStart)
	cleanupContainer := nodeAgentTemplate[cleanupStart:]
	require.Contains(t, cleanupContainer, "mountPath: /mnt/nvme")
	require.NotContains(t, cleanupContainer, "mountPath: /host")
	require.Contains(t, nodeAgentTemplate, "terminationGracePeriodSeconds: 180")
}

func TestQuickCacheOperatorPathIsProvisioned(t *testing.T) {
	okeCluster := readRepositoryFile(t, "terraform", "oke-cluster.tf")
	validation := readRepositoryFile(t, "terraform", "validation.tf")

	require.Contains(t, okeCluster, "var.install_quickcache")
	require.Contains(t, validation, "invalid_quickcache_deploy_path")
	require.Contains(t, validation, "requires a reachable OKE deployment path")
}
