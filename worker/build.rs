//! GTFS-realtime protobuf types, compiled from the spec's own .proto without needing `protoc`.

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("cargo:rerun-if-changed=proto/gtfs-realtime.proto");
    let fds = protox::compile(["proto/gtfs-realtime.proto"], ["proto"])?;
    prost_build::Config::new().compile_fds(fds)?;
    Ok(())
}
