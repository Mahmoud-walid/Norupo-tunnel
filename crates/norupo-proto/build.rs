use std::path::PathBuf;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Resolve the workspace root by walking up from this crate rather than by
    // canonicalising a relative path. On Windows, `canonicalize` returns an
    // extended-length `\\?\C:\...` path, and protoc cannot resolve a proto
    // file against an include root in that form - it reports
    // "File not found" for a file that is plainly there.
    let manifest_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let workspace_root = manifest_dir
        .parent()
        .and_then(std::path::Path::parent)
        .ok_or("norupo-proto must live two directories below the workspace root")?;

    let proto_root = workspace_root.join("proto");
    let proto_file = proto_root.join("norupo").join("v1").join("tunnel.proto");

    // Rebuild whenever the contract changes.
    println!("cargo:rerun-if-changed={}", proto_file.display());
    println!("cargo:rerun-if-changed=build.rs");

    // Vendor `protoc` so that building Norupo never requires a system protobuf
    // compiler. This is what keeps `cargo install norupo` working identically
    // on Windows, macOS and every Linux distro we ship to.
    if std::env::var_os("PROTOC").is_none() {
        std::env::set_var("PROTOC", protoc_bin_vendored::protoc_bin_path()?);
    }

    tonic_prost_build::configure()
        .build_server(true)
        .build_client(true)
        .bytes(".")
        .compile_protos(&[proto_file], &[proto_root])?;

    Ok(())
}
