# MemoryBackend

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**backend_type** | **BackendType** |  (enum: File, Uffd) |
**backend_path** | **String** | Based on 'backend_type' it is either 1) Path to the file that contains the guest memory to be loaded 2) Path to the UDS where a process is listening for a UFFD initialization control payload and open file descriptor that it can use to serve this process's guest memory page faults |

[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)
