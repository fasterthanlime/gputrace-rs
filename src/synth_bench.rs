//! Synthetic single-dispatch-per-pipeline workload used to reverse-engineer
//! per-command counter records in the .gpuprofiler_raw format.
//!
//! Each kernel runs a different fma-loop length so that "Kernel ALU
//! Instructions" and "Kernel Invocations" are unique per command. With one
//! dispatch per pipeline Xcode has nothing to aggregate, so any per-command
//! integer we see in the GPU Commands tab maps to exactly one stored record.

use std::path::PathBuf;

use crate::error::{Error, Result};

#[derive(Debug, Clone)]
pub struct SynthBenchOptions {
    pub output: PathBuf,
    pub iterations: Vec<u32>,
    pub threadgroup_counts: Vec<u32>,
    pub threads_per_group: u32,
}

/// Distinct loop-iteration counts. Mostly primes, all unique.
pub const DEFAULT_ITERATIONS: &[u32] = &[
    101, 197, 379, 547, 769, 1097, 1543, 2179, 3079, 4337, 6121, 8629, 12161, 17137, 24151, 34057,
];

/// Distinct threadgroup counts so `Kernel Invocations` is unique per command.
pub const DEFAULT_THREADGROUP_COUNTS: &[u32] = &[
    16, 23, 30, 37, 44, 51, 58, 65, 72, 79, 86, 93, 100, 107, 114, 121,
];

pub const DEFAULT_THREADS_PER_GROUP: u32 = 32;

impl Default for SynthBenchOptions {
    fn default() -> Self {
        Self {
            output: PathBuf::from("/tmp/synth.gputrace"),
            iterations: DEFAULT_ITERATIONS.to_vec(),
            threadgroup_counts: DEFAULT_THREADGROUP_COUNTS.to_vec(),
            threads_per_group: DEFAULT_THREADS_PER_GROUP,
        }
    }
}

#[derive(Debug, Clone)]
pub struct SynthBenchPlanRow {
    pub index: usize,
    pub function_name: String,
    pub iterations: u32,
    pub threadgroups: u32,
    pub threads_per_group: u32,
    pub invocations: u64,
}

#[derive(Debug, Clone)]
pub struct SynthBenchPlan {
    pub rows: Vec<SynthBenchPlanRow>,
}

pub fn plan(options: &SynthBenchOptions) -> Result<SynthBenchPlan> {
    if options.iterations.len() != options.threadgroup_counts.len() {
        return Err(Error::InvalidInput(
            "iterations and threadgroup_counts must have the same length".to_owned(),
        ));
    }
    let rows = options
        .iterations
        .iter()
        .zip(options.threadgroup_counts.iter())
        .enumerate()
        .map(|(index, (iterations, groups))| SynthBenchPlanRow {
            index,
            function_name: format!("synth_k{:02}", index),
            iterations: *iterations,
            threadgroups: *groups,
            threads_per_group: options.threads_per_group,
            invocations: u64::from(*groups) * u64::from(options.threads_per_group),
        })
        .collect();
    Ok(SynthBenchPlan { rows })
}

pub fn metal_source(iterations: &[u32]) -> String {
    let mut src = String::from(
        "#include <metal_stdlib>\n\
         using namespace metal;\n\n\
         template<uint N>\n\
         inline float synth_work(float seed) {\n\
         \x20   float x = seed;\n\
         \x20   for (uint j = 0; j < N; ++j) {\n\
         \x20       float c = 1.0f + float(j) * 1e-7f;\n\
         \x20       x = fma(x, c, 0.001f);\n\
         \x20   }\n\
         \x20   return x;\n\
         }\n\n",
    );
    for (i, n) in iterations.iter().enumerate() {
        src.push_str(&format!(
            "kernel void synth_k{i:02}(device float* out [[buffer(0)]],\n\
             \x20                     uint tid [[thread_position_in_grid]]) {{\n\
             \x20   out[tid] = synth_work<{n}>(float(tid) * 0.001f);\n\
             }}\n\n"
        ));
    }
    src
}

#[cfg(target_os = "macos")]
pub fn run(options: &SynthBenchOptions) -> Result<SynthBenchPlan> {
    use objc2::rc::autoreleasepool;
    use objc2::runtime::{AnyObject, ProtocolObject};
    use objc2_foundation::{NSString, NSURL};
    use objc2_metal::{
        MTLCaptureDescriptor, MTLCaptureDestination, MTLCaptureManager, MTLCommandBuffer,
        MTLCommandEncoder, MTLCommandQueue, MTLComputeCommandEncoder, MTLCreateSystemDefaultDevice,
        MTLDevice, MTLLibrary, MTLResourceOptions, MTLSize,
    };

    let plan_value = plan(options)?;

    autoreleasepool(|_pool| {
        let device = MTLCreateSystemDefaultDevice()
            .ok_or(Error::Unsupported("no Metal device available"))?;

        let source = metal_source(&options.iterations);
        let ns_source = NSString::from_str(&source);
        let library = device
            .newLibraryWithSource_options_error(&ns_source, None)
            .map_err(|error| {
                Error::InvalidInput(format!(
                    "failed to compile synth-bench Metal source: {}",
                    error.localizedDescription()
                ))
            })?;

        let mut pipelines = Vec::with_capacity(plan_value.rows.len());
        for row in &plan_value.rows {
            let function_name = NSString::from_str(&row.function_name);
            let function = library.newFunctionWithName(&function_name).ok_or_else(|| {
                Error::InvalidInput(format!("missing kernel function {}", row.function_name))
            })?;
            let pipeline = device
                .newComputePipelineStateWithFunction_error(&function)
                .map_err(|error| {
                    Error::InvalidInput(format!(
                        "failed to create pipeline for {}: {}",
                        row.function_name,
                        error.localizedDescription()
                    ))
                })?;
            pipelines.push(pipeline);
        }

        let queue = device
            .newCommandQueue()
            .ok_or(Error::Unsupported("failed to create MTLCommandQueue"))?;

        let max_invocations = plan_value
            .rows
            .iter()
            .map(|row| row.invocations as usize)
            .max()
            .unwrap_or(1);
        let buffer_len = max_invocations.max(1) * std::mem::size_of::<f32>();
        let out_buf = device
            .newBufferWithLength_options(buffer_len, MTLResourceOptions::StorageModeShared)
            .ok_or(Error::Unsupported("failed to allocate output MTLBuffer"))?;

        let capture_manager = unsafe { MTLCaptureManager::sharedCaptureManager() };
        if !capture_manager.supportsDestination(MTLCaptureDestination::GPUTraceDocument) {
            return Err(Error::Unsupported(
                "MTLCaptureDestinationGPUTraceDocument is not supported (set METAL_CAPTURE_ENABLED=1)",
            ));
        }
        if options.output.exists() {
            std::fs::remove_dir_all(&options.output)?;
        }
        let output_path = options
            .output
            .to_str()
            .ok_or_else(|| Error::InvalidInput("output path is not valid UTF-8".to_owned()))?;
        let url = NSURL::fileURLWithPath(&NSString::from_str(output_path));

        let descriptor = MTLCaptureDescriptor::new();
        let device_proto: &ProtocolObject<dyn MTLDevice> = device.as_ref();
        let device_object: &AnyObject = device_proto.as_ref();
        unsafe { descriptor.setCaptureObject(Some(device_object)) };
        descriptor.setDestination(MTLCaptureDestination::GPUTraceDocument);
        descriptor.setOutputURL(Some(&url));

        capture_manager
            .startCaptureWithDescriptor_error(&descriptor)
            .map_err(|error| {
                Error::InvalidInput(format!(
                    "MTLCaptureManager.startCapture failed: {}",
                    error.localizedDescription()
                ))
            })?;

        let cmd_buf = queue
            .commandBuffer()
            .ok_or(Error::Unsupported("failed to create MTLCommandBuffer"))?;
        let encoder = cmd_buf.computeCommandEncoder().ok_or(Error::Unsupported(
            "failed to create MTLComputeCommandEncoder",
        ))?;

        for (row, pipeline) in plan_value.rows.iter().zip(pipelines.iter()) {
            encoder.setComputePipelineState(pipeline);
            unsafe {
                encoder.setBuffer_offset_atIndex(Some(&out_buf), 0, 0);
            }
            let threadgroups = MTLSize {
                width: row.threadgroups as usize,
                height: 1,
                depth: 1,
            };
            let threads = MTLSize {
                width: row.threads_per_group as usize,
                height: 1,
                depth: 1,
            };
            encoder.dispatchThreadgroups_threadsPerThreadgroup(threadgroups, threads);
        }
        encoder.endEncoding();
        cmd_buf.commit();
        cmd_buf.waitUntilCompleted();

        capture_manager.stopCapture();

        Ok(plan_value)
    })
}

#[cfg(not(target_os = "macos"))]
pub fn run(_options: &SynthBenchOptions) -> Result<SynthBenchPlan> {
    Err(Error::Unsupported("synth-bench requires macOS"))
}

pub fn format_plan(plan: &SynthBenchPlan) -> String {
    let mut out = String::new();
    out.push_str("Synthetic dispatch plan\n");
    out.push_str("idx  function    iter   tg  threads  invocations\n");
    for row in &plan.rows {
        out.push_str(&format!(
            "{idx:>3}  {name:<10} {iter:>5} {tg:>4}  {threads:>7}  {invocations:>11}\n",
            idx = row.index,
            name = row.function_name,
            iter = row.iterations,
            tg = row.threadgroups,
            threads = row.threads_per_group,
            invocations = row.invocations,
        ));
    }
    out
}
