# SandboxEventHookRequest

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**id** | **uuid::Uuid** | Event identifier. | 
**version** | **String** | Event structure version. Always `v2`. | 
**r#type** | **String** | One of sandbox.lifecycle.created, sandbox.lifecycle.killed, sandbox.lifecycle.paused, sandbox.lifecycle.resumed, or sandbox.lifecycle.forked (an AgentENV extension). | 
**timestamp** | **chrono::DateTime<chrono::FixedOffset>** |  | 
**event_category** | **String** | Always `lifecycle`. | 
**event_label** | **String** | One of create, kill, pause, resume, or fork. | 
**event_data** | [**models::SandboxEventData**](SandboxEventData.md) |  | 
**sandbox_id** | **String** |  | 
**sandbox_execution_id** | **String** | Always empty. | 
**sandbox_template_id** | **String** | Snapshot or template the sandbox was launched from. | 
**sandbox_build_id** | **String** | Always empty. | 
**sandbox_team_id** | **uuid::Uuid** | Always the nil UUID; AgentENV is single-tenant. | 

[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


