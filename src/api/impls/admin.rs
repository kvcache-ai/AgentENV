use async_trait::async_trait;
use axum_extra::extract::CookieJar;
use headers::Host;
use http::Method;

use crate::observability::{DiskMetric, MachineInfo, NodeMetricsSnapshot, NodeSnapshot};
use agentenv_http_server::{apis::admin::*, models};

use super::ApiImpl;

impl From<MachineInfo> for models::MachineInfo {
    fn from(machine_info: MachineInfo) -> Self {
        models::MachineInfo::new(
            machine_info.cpu_family,
            machine_info.cpu_model,
            machine_info.cpu_model_name,
            machine_info.cpu_architecture,
        )
    }
}

impl From<DiskMetric> for models::DiskMetrics {
    fn from(disk: DiskMetric) -> Self {
        models::DiskMetrics::new(
            disk.mount_point,
            disk.device,
            disk.filesystem_type,
            disk.used_bytes,
            disk.total_bytes,
        )
    }
}

impl From<NodeMetricsSnapshot> for models::NodeMetrics {
    fn from(metrics: NodeMetricsSnapshot) -> Self {
        models::NodeMetrics::new(
            metrics.allocated_cpu,
            metrics.cpu_percent,
            metrics.cpu_count,
            metrics.allocated_memory_bytes,
            metrics.memory_used_bytes,
            metrics.memory_total_bytes,
            metrics
                .disks
                .into_iter()
                .map(models::DiskMetrics::from)
                .collect(),
            metrics.paused_allocated_cpu,
            metrics.paused_allocated_memory_bytes,
        )
    }
}

impl From<NodeSnapshot> for models::Node {
    fn from(node: NodeSnapshot) -> Self {
        models::Node::new(
            node.version,
            node.commit,
            node.node_id,
            node.service_instance_id,
            node.cluster_id.to_string(),
            node.machine_info.into(),
            models::NodeStatus::NodeStatusReady,
            node.sandbox_count,
            node.metrics.into(),
            node.create_successes,
            node.create_fails,
            node.sandbox_starting_count,
            node.paused_sandbox_count,
        )
    }
}

#[async_trait]
impl Admin<()> for ApiImpl {
    type Claims = super::Claims;

    async fn nodes_get(
        &self,
        _method: &Method,
        _host: &Host,
        _cookies: &CookieJar,
        _claims: &Self::Claims,
        query_params: &models::NodesGetQueryParams,
    ) -> Result<NodesGetResponse, ()> {
        let Some(observability) = self.observability() else {
            // When observability is disabled, the collection endpoint exposes
            // no nodes rather than returning a partial or synthetic record.
            return Ok(NodesGetResponse::Status200_SuccessfullyReturnedAllNodes(
                vec![],
            ));
        };
        if query_params
            .cluster_id
            .is_some_and(|cluster_id| cluster_id != observability.cluster_id())
        {
            return Ok(NodesGetResponse::Status200_SuccessfullyReturnedAllNodes(
                vec![],
            ));
        }
        let node = match observability.node_snapshot().await {
            Ok(node) => node,
            Err(err) => {
                return Ok(NodesGetResponse::Status500_ServerError(Self::error(
                    500,
                    err.to_string(),
                )));
            }
        };
        Ok(NodesGetResponse::Status200_SuccessfullyReturnedAllNodes(
            vec![models::Node::from(node)],
        ))
    }

    async fn nodes_node_id_get(
        &self,
        _method: &Method,
        _host: &Host,
        _cookies: &CookieJar,
        _claims: &Self::Claims,
        path_params: &models::NodesNodeIdGetPathParams,
        query_params: &models::NodesNodeIdGetQueryParams,
    ) -> Result<NodesNodeIdGetResponse, ()> {
        let Some(observability) = self.observability() else {
            // A disabled observability service behaves like node details are
            // unavailable on this process.
            return Ok(NodesNodeIdGetResponse::Status404_NotFound(Self::error(
                404,
                "observability is disabled on this node",
            )));
        };
        let cluster_mismatch = query_params
            .cluster_id
            .map(|cluster_id| cluster_id != observability.cluster_id())
            .unwrap_or(false);
        if path_params.node_id != observability.node_id() || cluster_mismatch {
            return Ok(NodesNodeIdGetResponse::Status404_NotFound(Self::error(
                404,
                format!("node {} not found", path_params.node_id),
            )));
        }

        let node = match observability.node_snapshot().await {
            Ok(node) => node,
            Err(err) => {
                return Ok(NodesNodeIdGetResponse::Status500_ServerError(Self::error(
                    500,
                    err.to_string(),
                )));
            }
        };

        let detail = models::NodeDetail::new(
            node.cluster_id.to_string(),
            node.version,
            node.commit,
            node.node_id,
            node.service_instance_id,
            node.machine_info.into(),
            models::NodeStatus::NodeStatusReady,
            node.sandbox_count,
            node.metrics.into(),
            node.create_successes,
            node.create_fails,
            node.paused_sandbox_count,
        );
        Ok(NodesNodeIdGetResponse::Status200_SuccessfullyReturnedTheNode(detail))
    }
}
