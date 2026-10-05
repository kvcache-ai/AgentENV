fn main() {
    println!("cargo:rerun-if-changed=services/api/proto/scheduler.proto");
    println!("cargo:rerun-if-changed=src/image/content.proto");
    println!("cargo:rerun-if-changed=src/image/build_history.proto");

    tonic_prost_build::configure()
        .build_server(false)
        .build_client(true)
        .compile_protos(
            &["services/api/proto/scheduler.proto"],
            &["services/api/proto"],
        )
        .expect("failed to compile scheduler proto for Rust gRPC client");

    tonic_prost_build::configure()
        .build_server(true)
        .compile_protos(
            &["src/image/content.proto", "src/image/build_history.proto"],
            &["src/image"],
        )
        .expect("failed to compile content store client");
}
