fn main() {
    // Use a vendored protoc so building needs nothing beyond cargo.
    std::env::set_var("PROTOC", protoc_bin_vendored::protoc_bin_path().unwrap());
    tonic_prost_build::compile_protos("proto/raftkv.proto").unwrap();
}
