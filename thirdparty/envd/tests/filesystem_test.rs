use anyhow::Result;
use envd::filesystem::{
    FileType, FilesystemClient, ListDirRequest, MakeDirRequest, MoveRequest, RemoveRequest,
    StatRequest,
};
use tonic::Request;

mod common;
use common::TEST_ENVD_ADDR;

/// Verifies filesystem operations: MakeDir, Stat, Move, ListDir, Remove.
#[tokio::test]
async fn test_filesystem_lifecycle() -> Result<()> {
    let mut client = FilesystemClient::connect(TEST_ENVD_ADDR, None).await?;

    let tmp = tempfile::TempDir::new()?;
    let base_path = tmp
        .path()
        .join("test_fs_lifecycle")
        .to_str()
        .unwrap()
        .to_string();
    let moved_path = tmp
        .path()
        .join("test_fs_lifecycle_moved")
        .to_str()
        .unwrap()
        .to_string();

    // 1. Ensure clean state (ignore errors)
    let _ = client
        .remove(Request::new(RemoveRequest {
            path: base_path.clone(),
        }))
        .await;
    let _ = client
        .remove(Request::new(RemoveRequest {
            path: moved_path.clone(),
        }))
        .await;

    // 2. MakeDir
    let _ = client
        .make_dir(Request::new(MakeDirRequest {
            path: base_path.clone(),
        }))
        .await?;

    // 3. Stat
    let stat_resp = client
        .stat(Request::new(StatRequest {
            path: base_path.clone(),
        }))
        .await?
        .into_inner();

    // Verify it is a directory
    assert_eq!(stat_resp.entry.unwrap().r#type, FileType::Directory as i32);

    // 4. Move
    let _ = client
        .r#move(Request::new(MoveRequest {
            source: base_path.clone(),
            destination: moved_path.clone(),
        }))
        .await?;

    // 5. Stat old path (should fail)
    let stat_old = client
        .stat(Request::new(StatRequest {
            path: base_path.clone(),
        }))
        .await;
    assert!(stat_old.is_err());

    // 6. Stat new path
    let stat_new = client
        .stat(Request::new(StatRequest {
            path: moved_path.clone(),
        }))
        .await?;
    assert_eq!(stat_new.into_inner().entry.unwrap().path, moved_path);

    // 6.1 Create subdirectories for ListDir test
    let sub1 = format!("{}/sub1", moved_path);
    let sub2 = format!("{}/sub2", moved_path);
    client
        .make_dir(Request::new(MakeDirRequest { path: sub1.clone() }))
        .await?;
    client
        .make_dir(Request::new(MakeDirRequest { path: sub2.clone() }))
        .await?;

    // 6.2 ListDir
    let list_resp = client
        .list_dir(Request::new(ListDirRequest {
            path: moved_path.clone(),
            depth: 1,
        }))
        .await?
        .into_inner();

    assert_eq!(
        list_resp.entries.len(),
        2,
        "Should list exactly 2 subdirectories"
    );
    let names: Vec<String> = list_resp.entries.iter().map(|e| e.name.clone()).collect();
    assert!(names.contains(&"sub1".to_string()));
    assert!(names.contains(&"sub2".to_string()));

    // 7. Remove
    let _ = client
        .remove(Request::new(RemoveRequest {
            path: moved_path.clone(),
        }))
        .await?;

    // 8. Stat new path (should fail)
    let stat_gone = client
        .stat(Request::new(StatRequest {
            path: moved_path.clone(),
        }))
        .await;
    assert!(stat_gone.is_err());

    Ok(())
}
