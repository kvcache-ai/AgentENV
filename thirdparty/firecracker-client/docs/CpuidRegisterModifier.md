# CpuidRegisterModifier

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**register** | **Register** | Target CPUID register name (enum: eax, ebx, ecx, edx) |
**bitmap** | **String** | 32-bit bitmap string defining which bits to modify. Format is \"0b\" followed by 32 characters where '0' = clear bit, '1' = set bit, 'x' = don't modify. Example \"0b00000000000000000000000000000001\" or \"0bxxxxxxxxxxxxxxxxxxxxxxxxxxxx0001\" |

[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)
