fn main() {
    println!("cargo:rerun-if-changed=../../schemas/proto/ryme/v1/kv.proto");
    tonic_build::configure()
        .build_server(true)
        .build_client(true)
        .compile_protos(
            &["../../schemas/proto/ryme/v1/kv.proto"],
            &["../../schemas/proto"],
        )
        .expect("prost codegen");
}
