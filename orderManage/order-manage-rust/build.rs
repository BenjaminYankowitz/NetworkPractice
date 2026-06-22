fn main() {
    println!("cargo::rerun-if-changed=../proto");
    protobuf_codegen::Codegen::new()
        .pure()
        // Use `protoc` parser, optional.
        // .protoc()
        // Use `protoc-bin-vendored` bundled protoc command, optional.
        // .protoc_path(&protoc_bin_vendored::protoc_bin_path().unwrap())
        // All inputs and imports from the inputs must reside in `includes` directories.
        .includes(&["../proto"])
        // Inputs must reside in some of include paths.
        .input("../proto/logMessages.proto")
        .input("../proto/marketMessages.proto")
        // Specify output directory relative to Cargo output directory.
        .cargo_out_dir("protos")
        .run_from_script();
}
