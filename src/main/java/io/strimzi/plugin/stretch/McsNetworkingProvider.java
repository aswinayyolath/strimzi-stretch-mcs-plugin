/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.plugin.stretch;

import io.fabric8.kubernetes.api.model.ConfigMap;
import io.fabric8.kubernetes.api.model.GenericKubernetesResource;
import io.fabric8.kubernetes.api.model.HasMetadata;
import io.fabric8.kubernetes.api.model.OwnerReference;
import io.fabric8.kubernetes.api.model.OwnerReferenceBuilder;
import io.fabric8.kubernetes.api.model.Service;
import io.fabric8.kubernetes.api.model.ServiceBuilder;
import io.fabric8.kubernetes.api.model.ServicePort;
import io.fabric8.kubernetes.api.model.ServicePortBuilder;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.strimzi.api.kafka.model.kafka.Kafka;
import io.strimzi.operator.cluster.operator.resource.ResourceOperatorSupplier;
import io.strimzi.operator.cluster.stretch.RemoteResourceOperatorSupplier;
import io.strimzi.operator.cluster.stretch.spi.StretchNetworkingProvider;
import io.strimzi.operator.common.Reconciliation;
import io.vertx.core.Future;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Multi-Cluster Services (MCS) API-based networking provider for
 * stretch clusters.
 *
 * Creates ClusterIP services and ServiceExport resources for DNS-based
 * cross-cluster communication.
 *
 * Advantages:
 * - DNS-based (no IP management)
 * - Standard Kubernetes API
 * - Works with multiple implementations (Cilium, Submariner, etc.)
 */
public final class McsNetworkingProvider
        implements StretchNetworkingProvider {

    /** Logger for this class. */
    private static final Logger LOGGER =
            LogManager.getLogger(McsNetworkingProvider.class);

    /** Standard Kafka replication port (internal, always 9091). */
    private static final int PORT_REPLICATION = 9091;
    /** Standard Kafka control plane port (internal, always 9090). */
    private static final int PORT_CONTROL_PLANE = 9090;

    /** Resource operator supplier for central cluster. */
    private ResourceOperatorSupplier centralSupplier;
    /** Resource operator supplier for remote clusters. */
    private RemoteResourceOperatorSupplier remoteResourceOperatorSupplier;
    /** Clusterset domain for MCS DNS names. */
    private String clustersetDomain = "clusterset.local";
    /** Whether to require namespace sameness across clusters. */
    private boolean requireNamespaceSameness = true;
    /** Track which services have been created to avoid duplicates per reconciliation. */
    private final Set<String> createdServices = new HashSet<>();
    /** Track the current reconciliation to clear cache when reconciliation changes. */
    private String currentReconciliation = null;
    /** Helper for ServiceExport operations in central cluster. */
    private ServiceExportHelper serviceExportHelper;
    /** Helpers for ServiceExport operations in remote clusters. */
    private final Map<String, ServiceExportHelper> remoteServiceExportHelpers = new HashMap<>();

    @Override
    public Future<Void> init(
            final Map<String, String> config,
            final ResourceOperatorSupplier centralSupplierParam,
            final RemoteResourceOperatorSupplier remoteResourceOperatorSupplierParam) {

        this.centralSupplier = centralSupplierParam;
        this.remoteResourceOperatorSupplier = remoteResourceOperatorSupplierParam;

        // Initialize ServiceExportHelper for central cluster
        KubernetesClient centralClient = centralSupplier.getKubernetesClient();
        this.serviceExportHelper = new ServiceExportHelper(centralClient);

        // Note: We don't validate CRD installation at startup to avoid permission issues
        // and timing problems. If ServiceExport CRD is missing, operations will fail
        // with clear error messages when actually needed.
        LOGGER.info("Initialized MCS networking provider - ServiceExport operations will be attempted as needed");

        // Initialize helpers for remote clusters
        for (Map.Entry<String, ResourceOperatorSupplier> entry :
                remoteResourceOperatorSupplier.remoteResourceOperators.entrySet()) {
            String clusterId = entry.getKey();
            ResourceOperatorSupplier supplier = entry.getValue();
            KubernetesClient remoteClient = supplier.getKubernetesClient();
            remoteServiceExportHelpers.put(clusterId, new ServiceExportHelper(remoteClient));
            LOGGER.debug("Initialized ServiceExportHelper for remote cluster: {}", clusterId);
        }

        // Read configuration
        if (config != null) {
            if (config.containsKey("clustersetDomain")) {
                this.clustersetDomain = config.get("clustersetDomain");
            }
            if (config.containsKey("requireNamespaceSameness")) {
                this.requireNamespaceSameness =
                        Boolean.parseBoolean(
                                config.get("requireNamespaceSameness"));
            }
        }

        LOGGER.info("MCS provider initialized with clustersetDomain={}, "
                + "requireNamespaceSameness={}",
                clustersetDomain, requireNamespaceSameness);
        return Future.succeededFuture();
    }

    @Override
    public Future<List<HasMetadata>> createNetworkingResources(
            final Reconciliation reconciliation,
            final String namespace,
            final String podName,
            final String clusterId,
            final Map<String, Integer> ports) {

        LOGGER.debug("{}: Creating MCS resources for pod {} in cluster {}",
                reconciliation, podName, clusterId);

        // Get the supplier for the target cluster
        final ResourceOperatorSupplier supplier;
        if (remoteResourceOperatorSupplier.remoteResourceOperators.containsKey(clusterId)) {
            supplier = remoteResourceOperatorSupplier.get(clusterId);
        } else {
            LOGGER.warn("{}: No supplier found for cluster {}, using central supplier",
                       reconciliation, clusterId);
            supplier = centralSupplier;
        }

        // Determine if this is the central cluster
        boolean isCentralCluster = (supplier == centralSupplier);

        // Service name is the SAME across all clusters: <kafka-cluster-name>-kafka-brokers
        // This allows MCS to aggregate them into a single ServiceImport
        String clusterName = reconciliation.name();
        String serviceName = clusterName + "-kafka-brokers";
        
        // Use clusterId to ensure we only create this service once per physical cluster
        String serviceKey = clusterId + "/" + namespace + "/" + serviceName;
        String reconciliationKey = reconciliation.toString();

        // Check if we've already created this service in this reconciliation
        // Since createNetworkingResources is called for EVERY pod, we need to deduplicate
        synchronized (createdServices) {
            // Clear cache if this is a new reconciliation
            if (currentReconciliation == null || !currentReconciliation.equals(reconciliationKey)) {
                LOGGER.debug("{}: New reconciliation detected, clearing createdServices cache (was: {})",
                           reconciliation, currentReconciliation);
                createdServices.clear();
                currentReconciliation = reconciliationKey;
            }
            
            // Check if we already processed this service in this reconciliation
            if (createdServices.contains(serviceKey)) {
                LOGGER.debug("{}: Service {} already processed in this reconciliation for cluster {} (serviceKey: {}), skipping creation",
                           reconciliation, serviceName, clusterId, serviceKey);
                return Future.succeededFuture(new ArrayList<>());
            }
            
            // Check if ServiceExport already exists in the cluster
            // This is critical for recovery - if it's missing, we'll create it
            ServiceExportHelper helper = remoteServiceExportHelpers.get(clusterId);
            if (helper == null) {
                helper = serviceExportHelper; // fallback to central
            }

            GenericKubernetesResource existingExport = helper.get(namespace, serviceName);
            if (existingExport != null) {
                LOGGER.debug("{}: ServiceExport {} already exists in cluster {}, skipping creation",
                           reconciliation, serviceName, clusterId);
                createdServices.add(serviceKey);
                return Future.succeededFuture(new ArrayList<>());
            }
            
            LOGGER.info("{}: ServiceExport {} does not exist in cluster {}, will create it",
                       reconciliation, serviceName, clusterId);
            createdServices.add(serviceKey);
        }

        List<ServicePort> servicePorts = ports.entrySet().stream()
            .map(entry -> {
                return new ServicePortBuilder()
                    .withName(entry.getKey())
                    .withPort(entry.getValue())
                    .withProtocol("TCP")
                    .build();
            })
            .collect(Collectors.toList());

        // Create headless service that selects ALL broker pods in this cluster
        // Selector: strimzi.io/cluster=<cluster-name>, strimzi.io/kind=Kafka, strimzi.io/name=<cluster-name>-kafka
        Service service = new ServiceBuilder()
            .withNewMetadata()
                .withName(serviceName)
                .withNamespace(namespace)
                .addToLabels("app", "strimzi")
                .addToLabels("strimzi.io/cluster", clusterName)
                .addToLabels("strimzi.io/kind", "Kafka")
                .addToLabels("strimzi.io/name", clusterName + "-kafka")
                .addToAnnotations("strimzi.io/stretch-cluster-id", clusterId)
            .endMetadata()
            .withNewSpec()
                .withType("ClusterIP")
                .withClusterIP("None") // Headless service
                .withPorts(servicePorts)
                // Selector matches ALL broker pods in this cluster
                .addToSelector("strimzi.io/cluster", clusterName)
                .addToSelector("strimzi.io/kind", "Kafka")
                .addToSelector("strimzi.io/name", clusterName + "-kafka")
            .endSpec()
            .build();

        // Get ServiceExportHelper for this cluster
        ServiceExportHelper helper = remoteServiceExportHelpers.get(clusterId);
        if (helper == null) {
            helper = serviceExportHelper; // fallback to central
        }

        // Create ServiceExport for this service
        Map<String, String> labels = new HashMap<>();
        labels.put("app", "strimzi");
        labels.put("strimzi.io/cluster", clusterName);

        Map<String, String> annotations = new HashMap<>();
        annotations.put("strimzi.io/stretch-cluster-id", clusterId);

        GenericKubernetesResource serviceExport = helper.create(
            serviceName, namespace, labels, annotations);

        // Set owner reference based on cluster type
        if (isCentralCluster) {
            // For central cluster, set Kafka CR as owner
            try {
                HasMetadata kafkaCr = supplier.getKubernetesClient()
                    .resources(Kafka.class)
                    .inNamespace(namespace)
                    .withName(clusterName)
                    .get();
                
                if (kafkaCr != null && kafkaCr.getMetadata().getUid() != null) {
                    OwnerReference kafkaOwner = 
                        new OwnerReferenceBuilder()
                            .withApiVersion("kafka.strimzi.io/v1beta2")
                            .withKind("Kafka")
                            .withName(clusterName)
                            .withUid(kafkaCr.getMetadata().getUid())
                            .withController(false)
                            .withBlockOwnerDeletion(false)
                            .build();
                    
                    List<OwnerReference> owners = new ArrayList<>();
                    owners.add(kafkaOwner);
                    serviceExport.getMetadata().setOwnerReferences(owners);
                    
                    LOGGER.debug("{}: Added Kafka CR {} (UID: {}) as owner for ServiceExport {} in central cluster",
                               reconciliation, clusterName, kafkaCr.getMetadata().getUid(), serviceName);
                } else {
                    LOGGER.warn("{}: Kafka CR {} not found or has no UID in central cluster, ServiceExport will not have owner reference",
                               reconciliation, clusterName);
                }
            } catch (Exception e) {
                LOGGER.warn("{}: Failed to fetch Kafka CR {} in central cluster: {}",
                           reconciliation, clusterName, e.getMessage());
            }
        } else {
            // For remote clusters, set GC ConfigMap as owner
            String gcConfigMapName = clusterName + "-kafka-gc";
            
            // Fetch the GC ConfigMap to get its UID
            ConfigMap gcConfigMap = null;
            try {
                gcConfigMap = supplier.configMapOperations.get(namespace, gcConfigMapName);
            } catch (Exception e) {
                LOGGER.warn("{}: Failed to fetch GC ConfigMap {} in cluster {}: {}",
                           reconciliation, gcConfigMapName, clusterId, e.getMessage());
            }
            
            if (gcConfigMap != null && gcConfigMap.getMetadata().getUid() != null) {
                String gcUid = gcConfigMap.getMetadata().getUid();
                
                OwnerReference gcOwner = 
                    new OwnerReferenceBuilder()
                        .withApiVersion("v1")
                        .withKind("ConfigMap")
                        .withName(gcConfigMapName)
                        .withUid(gcUid)
                        .withController(false)
                        .withBlockOwnerDeletion(false)
                        .build();
                
                List<OwnerReference> owners = new ArrayList<>();
                owners.add(gcOwner);
                serviceExport.getMetadata().setOwnerReferences(owners);
                
                LOGGER.debug("{}: Added GC ConfigMap {} (UID: {}) as owner for ServiceExport {} in cluster {}",
                           reconciliation, gcConfigMapName, gcUid, serviceName, clusterId);
            } else {
                LOGGER.warn("{}: GC ConfigMap {} not found or has no UID in cluster {}, ServiceExport will not have owner reference",
                           reconciliation, gcConfigMapName, clusterId);
            }
        }

        List<HasMetadata> resources = new ArrayList<>();

        // IMPORTANT: For central cluster, skip Service creation (Strimzi already creates it)
        // but still create ServiceExport. For remote clusters, create both.
        Future<Void> serviceFuture;
        if (isCentralCluster) {
            LOGGER.debug("{}: Skipping Service creation for {} in central cluster {} (Strimzi creates it)",
                       reconciliation, serviceName, clusterId);
            serviceFuture = Future.succeededFuture();
        } else {
            // Create service in remote cluster
            serviceFuture = supplier.serviceOperations
                .reconcile(reconciliation, namespace, serviceName, service)
                .compose(serviceResult -> {
                    resources.add(service);
                    LOGGER.debug("{}: Created/updated service {} in remote cluster {}",
                               reconciliation, serviceName, clusterId);
                    return Future.succeededFuture();
                });
        }

        // Create ServiceExport in ALL clusters (including central)
        final ServiceExportHelper finalHelper = helper;
        return serviceFuture.compose(v -> {
                try {
                    // Create ServiceExport in the cluster
                    LOGGER.debug("{}: Creating ServiceExport {} in cluster {}",
                               reconciliation, serviceName, clusterId);
                    finalHelper.createOrReplace(serviceExport);
                    resources.add(serviceExport);
                    LOGGER.debug("{}: Created/updated ServiceExport {} in cluster {}",
                               reconciliation, serviceName, clusterId);
                    return Future.succeededFuture(resources);
                } catch (Exception error) {
                    LOGGER.error("{}: Failed to create ServiceExport {} in cluster {}: {}",
                               reconciliation, serviceName, clusterId, error.getMessage(), error);
                    return Future.succeededFuture(resources);
                }
            });
    }

    /**
     * Creates a ServiceExport resource for MCS-based stretch clusters.
     * This is a helper method for creating ServiceExport resources outside
     * of the main networking resource creation flow.
     *
     * @param serviceName Name of the service to export
     * @param namespace Namespace where the service exists
     * @param labels Labels to apply to the ServiceExport
     * @param ownerReferences Owner references (empty list for remote
     *                        resources)
     * @return ServiceExport resource
     */
    public GenericKubernetesResource createServiceExport(
            final String serviceName,
            final String namespace,
            final Map<String, String> labels,
            final List<OwnerReference> ownerReferences) {

        GenericKubernetesResource serviceExport = serviceExportHelper.create(
            serviceName, namespace, labels, null);

        if (ownerReferences != null && !ownerReferences.isEmpty()) {
            serviceExport.getMetadata().setOwnerReferences(ownerReferences);
        }

        return serviceExport;
    }

    @Override
    public String generateServiceDnsName(final String namespace,
                                          final String serviceName,
                                          final String clusterId) {
        // MCS format: <service>.<cluster-id>.<namespace>.svc.<clusterset-domain>
        return String.format("%s.%s.%s.svc.%s",
            serviceName,
            clusterId,
            namespace,
            clustersetDomain);
    }

    @Override
    public String generatePodDnsName(final String namespace,
                                      final String serviceName,
                                      final String podName,
                                      final String clusterId) {
        // MCS format: <pod>.<cluster-id>.<service>.<namespace>.svc.<clusterset-domain>
        String dns = String.format("%s.%s.%s.%s.svc.%s",
            podName,
            clusterId,
            serviceName,
            namespace,
            clustersetDomain);
        
        LOGGER.debug("Generated MCS DNS for pod {}: {}", podName, dns);
        
        return dns;
    }

    @Override
    public Future<String> discoverPodEndpoint(
            final Reconciliation reconciliation,
            final String namespace,
            final String podName,
            final String clusterId,
            final String portName) {

        String serviceName = podName + "-mcs";

        // Get the service to find the port number
        return centralSupplier.serviceOperations
            .getAsync(namespace, serviceName)
            .map(service -> {
                if (service == null) {
                    throw new RuntimeException(
                            "MCS service not found: " + serviceName);
                }

                // Find the port number for the requested port name
                ServicePort port = service.getSpec().getPorts().stream()
                    .filter(p -> p.getName().equals(portName))
                    .findFirst()
                    .orElseThrow(() -> new RuntimeException(
                            "Port not found: " + portName));

                // Generate MCS-compliant DNS name
                // Format: <pod>.<cluster-id>.<service>.<namespace>.svc.<clusterset-domain>:<port>
                String dnsName = String.format("%s.%s.%s.%s.svc.%s:%d",
                    podName,
                    clusterId,
                    serviceName,
                    namespace,
                    clustersetDomain,
                    port.getPort()
                );

                return dnsName;
            });
    }

    @Override
    public Future<String> generateAdvertisedListeners(
            final Reconciliation reconciliation,
            final String namespace,
            final String podName,
            final String clusterId,
            final Map<String, String> listeners) {

        // For MCS, we generate DNS names directly without needing to
        // discover services
        // MCS DNS format: <pod>.<cluster-id>.<service>.<namespace>
        // .svc.<clusterset-domain>:<port>

        List<String> listenerStrings = new ArrayList<>();

        // Get the Kafka cluster name from the Reconciliation (this is the Kafka CR name)
        String clusterName = reconciliation.name();
        // Use headless service name: <cluster-name>-kafka-brokers
        String serviceName = clusterName + "-kafka-brokers";

        for (Map.Entry<String, String> entry
                : listeners.entrySet()) {
            String listenerName = entry.getKey(); // e.g., "REPLICATION-9091", "PLAIN-9092"
            String portName = entry.getValue();   // e.g., "replication", "plain"

            // Extract port number from listener name (format: LISTENER_NAME-PORT)
            int port = extractPortFromListenerName(listenerName);

            // Generate MCS DNS name
            String dnsName = String.format("%s.%s.%s.%s.svc.%s",
                podName,
                clusterId,
                serviceName,
                namespace,
                clustersetDomain
            );

            // Format: LISTENER_NAME://dns:port
            String listener = String.format("%s://%s:%d",
                    listenerName, dnsName, port);
            listenerStrings.add(listener);

            LOGGER.debug(
                    "{}: Generated advertised listener for {}: {}",
                    reconciliation, podName, listener);
        }

        return Future.succeededFuture(String.join(",", listenerStrings));
    }

    /**
     * Extract port number from listener name.
     * Listener names follow the format: LISTENER_NAME-PORT (e.g., "REPLICATION-9091", "PLAIN-9092").
     * For internal ports (replication, control-plane), use standard ports if not specified.
     *
     * @param listenerName Listener name that may contain port number (e.g., "REPLICATION-9091")
     * @return Port number extracted from listener name
     * @throws IllegalArgumentException if port cannot be extracted
     */
    private int extractPortFromListenerName(final String listenerName) {
        // Try to extract port from listener name (format: LISTENER_NAME-PORT)
        String[] parts = listenerName.split("-");
        
        // Check last part for port number
        if (parts.length > 0) {
            String lastPart = parts[parts.length - 1];
            if (lastPart.matches("\\d+")) {
                try {
                    return Integer.parseInt(lastPart);
                } catch (NumberFormatException e) {
                    // Fall through
                }
            }
        }
        
        // Fallback for internal ports without explicit port in name
        String lowerName = listenerName.toLowerCase(Locale.ROOT);
        if (lowerName.contains("replication")) {
            return PORT_REPLICATION;
        } else if (lowerName.contains("control") || lowerName.contains("ctrlplane")) {
            return PORT_CONTROL_PLANE;
        }
        
        throw new IllegalArgumentException(
            "Cannot extract port number from listener name: " + listenerName + 
            ". Expected format: LISTENER_NAME-PORT (e.g., PLAIN-9092)");
    }

    @Override
    public Future<String> generateQuorumVoters(
            final Reconciliation reconciliation,
            final String namespace,
            final List<ControllerPodInfo> controllerPods,
            final String replicationPortName) {

        // For MCS, generate quorum voters directly using DNS names
        // Format: <nodeId>@<pod>.<cluster-id>.<service>.<namespace>
        // .svc.<clusterset-domain>:<port>

        List<String> voters = new ArrayList<>();
        // Replication port is always 9091 for internal Kafka communication
        int port = PORT_REPLICATION;

        for (ControllerPodInfo controller : controllerPods) {
            String podName = controller.podName();
            String clusterId = controller.clusterId();
            String serviceName = podName; // For per-pod services

            // Generate MCS DNS name
            String dnsName = String.format("%s.%s.%s.%s.svc.%s",
                podName,
                clusterId,
                serviceName,
                namespace,
                clustersetDomain
            );

            // Format: nodeId@dns:port
            String voter = String.format("%d@%s:%d",
                controller.nodeId(), dnsName, port);
            voters.add(voter);

            LOGGER.debug("{}: Generated quorum voter: {}",
                    reconciliation, voter);
        }

        String quorumVoters = String.join(",", voters);
        LOGGER.debug("{}: Generated controller.quorum.voters: {}",
                reconciliation, quorumVoters);

        return Future.succeededFuture(quorumVoters);
    }

    @Override
    public Future<Void> deleteNetworkingResources(
            final Reconciliation reconciliation,
            final String namespace,
            final String podName,
            final String clusterId) {

        String serviceName = podName + "-mcs";

        LOGGER.debug(
                "{}: Deleting MCS resources {} in cluster {}",
                reconciliation, serviceName, clusterId);

        // Delete ServiceExport first
        try {
            serviceExportHelper.delete(namespace, serviceName);
            LOGGER.debug("{}: Deleted ServiceExport {} in cluster {}",
                       reconciliation, serviceName, clusterId);
        } catch (Exception e) {
            LOGGER.warn("{}: Failed to delete ServiceExport {}: {}",
                       reconciliation, serviceName, e.getMessage());
        }

        // Then delete Service
        return centralSupplier.serviceOperations
            .reconcile(reconciliation, namespace, serviceName, null)
            .mapEmpty();
    }

    @Override
    public String getProviderName() {
        return "mcs";
    }

}
