# PatchCustomExtensionParamsHookRequest

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**sandbox_id** | **String** |  | 
**patch** | Option<**serde_json::Value**> | Opaque JSON patch document passed through verbatim from the user's PATCH request. Its semantics are defined entirely by the extension. | 
**metadata** | Option<**std::collections::HashMap<String, String>**> | User-provided sandbox metadata from the create-sandbox request. Only the launch-provided value is sent: metadata is never inherited from a source snapshot. It is retained across pause/resume of the same sandbox. | [optional]

[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)


