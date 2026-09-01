# GuestRegionUffdMapping

## Properties

Name | Type | Description | Notes
------------ | ------------- | ------------- | -------------
**base_host_virt_addr** | **i64** | Base host virtual address of the guest memory region. |
**guest_phys_addr** | **i64** | Guest physical address at which this region begins. Unlike offset, this preserves GPA holes. |
**size** | **i64** | Region size in bytes. |
**offset** | **i64** | Cumulative byte offset of this region within a contiguous layout of all guest memory regions. This is the position where this region's data starts if all regions are concatenated in iteration order, matching the offset semantics used in the uffd restore path (snapshot file layout). Note this is NOT the Guest Physical Address — GPAs may contain gaps (e.g. the MMIO hole near 4 GiB) absent from the contiguous layout. |
**page_size** | **i32** | Page size for this region (typically 4096). |
**page_size_kib** | Option<**i32**> | Deprecated. Same value as page_size. Will be removed in 2.0. | [optional]

[[Back to Model list]](../README.md#documentation-for-models) [[Back to API list]](../README.md#documentation-for-api-endpoints) [[Back to README]](../README.md)
