use std::path::PathBuf;

use serde::Serialize;

use crate::error::{Error, Result};
use crate::profiler;
use crate::trace::TraceBundle;

#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct XcodeMioReport {
    pub trace_source: PathBuf,
    pub profiler_directory: PathBuf,
    pub stream_data_path: PathBuf,
    pub framework_path: PathBuf,
    pub gpu_command_count: usize,
    pub encoder_count: usize,
    pub pipeline_state_count: usize,
    pub draw_count: usize,
    pub cost_record_count: usize,
    pub gpu_time_ns: u64,
    pub pipelines: Vec<XcodeMioPipeline>,
    pub gpu_commands: Vec<XcodeMioGpuCommand>,
    pub warnings: Vec<String>,
}

#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct XcodeMioPipeline {
    pub index: usize,
    pub object_id: u64,
    pub pointer_id: u64,
    pub function_index: u64,
    pub gpu_command_count: usize,
    pub pipeline_address: Option<u64>,
    pub function_name: Option<String>,
}

#[derive(Debug, Clone, Serialize, PartialEq)]
pub struct XcodeMioGpuCommand {
    pub index: usize,
    pub function_index: u64,
    pub sub_command_index: i32,
    pub encoder_index: usize,
    pub pipeline_index: usize,
    pub pipeline_object_id: u64,
    pub command_buffer_index: usize,
    pub function_name: Option<String>,
}

pub fn report(trace: &TraceBundle) -> Result<XcodeMioReport> {
    let profiler_directory = profiler::find_profiler_directory(&trace.path)
        .ok_or_else(|| Error::NotFound(trace.path.clone()))?;
    let stream_data_path = profiler_directory.join("streamData");
    if !stream_data_path.is_file() {
        return Err(Error::NotFound(stream_data_path));
    }

    let profiler_summary = profiler::stream_data_summary(&trace.path).ok();
    platform::decode(
        trace.path.clone(),
        profiler_directory,
        stream_data_path,
        profiler_summary.as_ref(),
    )
}

pub fn format_report(report: &XcodeMioReport) -> String {
    let mut out = String::new();
    out.push_str("Xcode private MIO decode\n");
    out.push_str(&format!(
        "streamData: {}\nframework: {}\n\n",
        report.stream_data_path.display(),
        report.framework_path.display()
    ));
    out.push_str(&format!(
        "gpu_commands={} encoders={} pipelines={} draws={} cost_records={} gpu_time={:.3} ms\n\n",
        report.gpu_command_count,
        report.encoder_count,
        report.pipeline_state_count,
        report.draw_count,
        report.cost_record_count,
        report.gpu_time_ns as f64 / 1_000_000.0
    ));

    out.push_str("Pipelines by command count:\n");
    let mut pipelines = report.pipelines.clone();
    pipelines.sort_by(|left, right| {
        right
            .gpu_command_count
            .cmp(&left.gpu_command_count)
            .then_with(|| left.index.cmp(&right.index))
    });
    for pipeline in pipelines {
        let name = pipeline
            .function_name
            .as_deref()
            .unwrap_or("<unknown function>");
        let address = pipeline
            .pipeline_address
            .map(|value| format!("0x{value:x}"))
            .unwrap_or_else(|| "-".to_owned());
        out.push_str(&format!(
            "  {:>2} {:>4} commands  {:<56} addr={} object_id={}\n",
            pipeline.index, pipeline.gpu_command_count, name, address, pipeline.object_id
        ));
    }

    if !report.warnings.is_empty() {
        out.push_str("\nWarnings:\n");
        for warning in &report.warnings {
            out.push_str(&format!("  - {warning}\n"));
        }
    }

    out
}

#[cfg(target_os = "macos")]
mod platform {
    use std::ffi::{CString, c_char, c_int, c_void};
    use std::mem;
    use std::path::PathBuf;

    use super::{XcodeMioGpuCommand, XcodeMioPipeline, XcodeMioReport};
    use crate::error::{Error, Result};
    use crate::profiler;

    type Id = *mut c_void;
    type Class = *mut c_void;
    type Sel = *mut c_void;
    type CfTypeRef = *const c_void;
    type CfStringRef = *const c_void;

    const RTLD_NOW: c_int = 0x2;
    const RTLD_GLOBAL: c_int = 0x8;
    const K_CF_STRING_ENCODING_UTF8: u32 = 0x0800_0100;
    const GT_SHADER_PROFILER_FRAMEWORK: &str = "/Applications/Xcode.app/Contents/PlugIns/GPUDebugger.ideplugin/Contents/Frameworks/GTShaderProfiler.framework/Versions/A/GTShaderProfiler";
    const MTL_TOOLS_SHADER_PROFILER_FRAMEWORK: &str = "/Applications/Xcode.app/Contents/SharedFrameworks/MTLToolsShaderProfiler.framework/Versions/A/MTLToolsShaderProfiler";

    unsafe extern "C" {
        fn dlopen(path: *const c_char, mode: c_int) -> *mut c_void;
        fn open(path: *const c_char, oflag: c_int, ...) -> c_int;
        fn dup(fd: c_int) -> c_int;
        fn dup2(src: c_int, dst: c_int) -> c_int;
        fn close(fd: c_int) -> c_int;
        fn objc_lookUpClass(name: *const c_char) -> Class;
        fn sel_registerName(name: *const c_char) -> Sel;
        fn objc_msgSend();
        fn CFStringCreateWithCString(
            alloc: CfTypeRef,
            c_str: *const c_char,
            encoding: u32,
        ) -> CfStringRef;
        fn CFRelease(cf: CfTypeRef);
    }

    #[link(name = "Foundation", kind = "framework")]
    unsafe extern "C" {}

    #[link(name = "objc")]
    unsafe extern "C" {}

    pub fn decode(
        trace_source: PathBuf,
        profiler_directory: PathBuf,
        stream_data_path: PathBuf,
        profiler_summary: Option<&profiler::ProfilerStreamDataSummary>,
    ) -> Result<XcodeMioReport> {
        let framework_path = PathBuf::from(GT_SHADER_PROFILER_FRAMEWORK);
        let silence = FdSilencer::new();
        let mut runtime = unsafe { Runtime::load()? };
        let stream = runtime.stream_data(&stream_data_path)?;
        let processor = unsafe { runtime.processor(stream)? };
        unsafe {
            runtime.send_void(processor, "processStreamData")?;
            runtime.send_void(processor, "waitUntilFinished")?;
        }
        let mio = unsafe { runtime.send_id(processor, "mioData")? };
        let result = unsafe { runtime.send_id(processor, "result")? };
        let shader_result = unsafe { runtime.send_id(result, "shaderProfilerResult")? };
        let gpu_commands = unsafe { runtime.send_id(shader_result, "gpuCommands")? };
        let pipelines = unsafe { runtime.send_id(shader_result, "pipelineStates")? };
        let encoders = unsafe { runtime.send_id(shader_result, "encoders")? };

        let pipeline_count = unsafe { runtime.array_count(pipelines)? };
        let command_count = unsafe { runtime.array_count(gpu_commands)? };
        let encoder_count = unsafe { runtime.array_count(encoders)? };
        let mut decoded_pipelines = Vec::with_capacity(pipeline_count);
        for index in 0..pipeline_count {
            let pipeline = unsafe { runtime.array_object(pipelines, index)? };
            let summary_pipeline =
                profiler_summary.and_then(|summary| summary.pipelines.get(index));
            decoded_pipelines.push(XcodeMioPipeline {
                index: unsafe { runtime.send_u32(pipeline, "index")? as usize },
                object_id: unsafe { runtime.send_u64(pipeline, "objectId")? },
                pointer_id: unsafe { runtime.send_u64(pipeline, "pointerId")? },
                function_index: unsafe { runtime.send_u64(pipeline, "functionIndex")? },
                gpu_command_count: unsafe {
                    runtime.send_u32(pipeline, "numGPUCommands")? as usize
                },
                pipeline_address: summary_pipeline.map(|pipeline| pipeline.pipeline_address),
                function_name: summary_pipeline.and_then(|pipeline| pipeline.function_name.clone()),
            });
        }

        let mut decoded_commands = Vec::with_capacity(command_count);
        for index in 0..command_count {
            let command = unsafe { runtime.array_object(gpu_commands, index)? };
            let pipeline_index =
                unsafe { runtime.send_u32(command, "pipelineInfoIndex")? as usize };
            let function_name = profiler_summary
                .and_then(|summary| summary.pipelines.get(pipeline_index))
                .and_then(|pipeline| pipeline.function_name.clone());
            decoded_commands.push(XcodeMioGpuCommand {
                index: unsafe { runtime.send_u32(command, "index")? as usize },
                function_index: unsafe { runtime.send_u64(command, "functionIndex")? },
                sub_command_index: unsafe { runtime.send_i32(command, "subCommandIndex")? },
                encoder_index: unsafe { runtime.send_u32(command, "encoderInfoIndex")? as usize },
                pipeline_index,
                pipeline_object_id: unsafe { runtime.send_u64(command, "pipelineStateObjectId")? },
                command_buffer_index: unsafe {
                    runtime.send_u32(command, "commandBufferIndex")? as usize
                },
                function_name,
            });
        }

        let mut warnings = Vec::new();
        if profiler_summary.is_some_and(|summary| summary.num_gpu_commands != command_count) {
            warnings.push("private MIO command count differs from streamData summary".to_owned());
        }
        if decoded_pipelines
            .iter()
            .any(|pipeline| pipeline.function_name.is_none())
        {
            warnings.push("one or more private MIO pipelines could not be named".to_owned());
        }

        let report = XcodeMioReport {
            trace_source,
            profiler_directory,
            stream_data_path,
            framework_path,
            gpu_command_count: command_count,
            encoder_count,
            pipeline_state_count: pipeline_count,
            draw_count: unsafe { runtime.send_u64(mio, "drawCount")? as usize },
            cost_record_count: unsafe { runtime.send_u64(mio, "costCount")? as usize },
            gpu_time_ns: unsafe { runtime.send_u64(mio, "gpuTime")? },
            pipelines: decoded_pipelines,
            gpu_commands: decoded_commands,
            warnings,
        };
        drop(runtime);
        std::thread::sleep(std::time::Duration::from_millis(100));
        drop(silence);
        Ok(report)
    }

    struct Runtime {
        pool: Id,
    }

    impl Runtime {
        unsafe fn load() -> Result<Self> {
            unsafe {
                let _ = load_framework(MTL_TOOLS_SHADER_PROFILER_FRAMEWORK);
                load_framework(GT_SHADER_PROFILER_FRAMEWORK)?;
                let pool_class = lookup_class("NSAutoreleasePool")?;
                let pool = send_id(send_id(pool_class, "alloc")?, "init")?;
                Ok(Self { pool })
            }
        }

        fn stream_data(&mut self, stream_data_path: &std::path::Path) -> Result<Id> {
            let path = stream_data_path.to_string_lossy();
            let path = CString::new(path.as_bytes())
                .map_err(|_| Error::InvalidInput("streamData path contains NUL".to_owned()))?;
            unsafe {
                let path = CfString::new(path.as_ptr())?;
                let url_class = lookup_class("NSURL")?;
                let url = send_id_id(url_class, "fileURLWithPath:", path.ptr.cast_mut())?;
                let stream_class = lookup_class("GTShaderProfilerStreamData")?;
                send_id_id(stream_class, "dataFromArchivedDataURL:", url)
            }
        }

        unsafe fn processor(&mut self, stream: Id) -> Result<Id> {
            unsafe {
                let processor_class = lookup_class("GTShaderProfilerStreamDataProcessor")?;
                let processor = send_id(processor_class, "alloc")?;
                send_id_id_id(
                    processor,
                    "initWithStreamData:llvmHelperPath:",
                    stream,
                    std::ptr::null_mut(),
                )
            }
        }

        unsafe fn send_id(&mut self, receiver: Id, selector: &str) -> Result<Id> {
            unsafe { send_id(receiver, selector) }
        }

        unsafe fn send_void(&mut self, receiver: Id, selector: &str) -> Result<()> {
            unsafe { send_void(receiver, selector) }
        }

        unsafe fn send_u64(&mut self, receiver: Id, selector: &str) -> Result<u64> {
            unsafe { send_u64(receiver, selector) }
        }

        unsafe fn send_u32(&mut self, receiver: Id, selector: &str) -> Result<u32> {
            unsafe { send_u32(receiver, selector) }
        }

        unsafe fn send_i32(&mut self, receiver: Id, selector: &str) -> Result<i32> {
            unsafe { send_i32(receiver, selector) }
        }

        unsafe fn array_count(&mut self, array: Id) -> Result<usize> {
            unsafe { Ok(send_u64(array, "count")? as usize) }
        }

        unsafe fn array_object(&mut self, array: Id, index: usize) -> Result<Id> {
            unsafe { send_id_usize(array, "objectAtIndex:", index) }
        }
    }

    struct FdSilencer {
        stdout_fd: c_int,
        stderr_fd: c_int,
    }

    impl FdSilencer {
        fn new() -> Self {
            unsafe {
                let dev_null = c"/dev/null";
                let null_fd = open(dev_null.as_ptr(), 1);
                if null_fd < 0 {
                    return Self {
                        stdout_fd: -1,
                        stderr_fd: -1,
                    };
                }
                let stdout_fd = dup(1);
                let stderr_fd = dup(2);
                if stdout_fd >= 0 {
                    let _ = dup2(null_fd, 1);
                }
                if stderr_fd >= 0 {
                    let _ = dup2(null_fd, 2);
                }
                let _ = close(null_fd);
                Self {
                    stdout_fd,
                    stderr_fd,
                }
            }
        }
    }

    impl Drop for FdSilencer {
        fn drop(&mut self) {
            unsafe {
                if self.stdout_fd >= 0 {
                    let _ = dup2(self.stdout_fd, 1);
                    let _ = close(self.stdout_fd);
                }
                if self.stderr_fd >= 0 {
                    let _ = dup2(self.stderr_fd, 2);
                    let _ = close(self.stderr_fd);
                }
            }
        }
    }

    impl Drop for Runtime {
        fn drop(&mut self) {
            if !self.pool.is_null() {
                unsafe {
                    let _ = send_void(self.pool, "drain");
                }
            }
        }
    }

    struct CfString {
        ptr: CfStringRef,
    }

    impl CfString {
        unsafe fn new(value: *const c_char) -> Result<Self> {
            let ptr = unsafe {
                CFStringCreateWithCString(std::ptr::null(), value, K_CF_STRING_ENCODING_UTF8)
            };
            if ptr.is_null() {
                Err(Error::InvalidInput("failed to create CFString".to_owned()))
            } else {
                Ok(Self { ptr })
            }
        }
    }

    impl Drop for CfString {
        fn drop(&mut self) {
            unsafe {
                CFRelease(self.ptr);
            }
        }
    }

    unsafe fn load_framework(path: &str) -> Result<()> {
        let path = CString::new(path).expect("framework path contains no NUL");
        let handle = unsafe { dlopen(path.as_ptr(), RTLD_NOW | RTLD_GLOBAL) };
        if handle.is_null() {
            Err(Error::NotFound(PathBuf::from(
                path.to_string_lossy().into_owned(),
            )))
        } else {
            Ok(())
        }
    }

    unsafe fn lookup_class(name: &str) -> Result<Class> {
        let name = CString::new(name).expect("class name contains no NUL");
        let class = unsafe { objc_lookUpClass(name.as_ptr()) };
        if class.is_null() {
            Err(Error::InvalidInput(format!(
                "Objective-C class {} is not available",
                name.to_string_lossy()
            )))
        } else {
            Ok(class)
        }
    }

    unsafe fn selector(name: &str) -> Result<Sel> {
        let name = CString::new(name)
            .map_err(|_| Error::InvalidInput("selector contains NUL".to_owned()))?;
        let sel = unsafe { sel_registerName(name.as_ptr()) };
        if sel.is_null() {
            Err(Error::InvalidInput(format!(
                "Objective-C selector {} is not available",
                name.to_string_lossy()
            )))
        } else {
            Ok(sel)
        }
    }

    unsafe fn send_id(receiver: Id, sel: &str) -> Result<Id> {
        if receiver.is_null() {
            return Err(Error::InvalidInput(format!(
                "nil Objective-C receiver for {sel}"
            )));
        }
        let sel = unsafe { selector(sel)? };
        let f: extern "C" fn(Id, Sel) -> Id = unsafe { mem::transmute(objc_msgSend as *const ()) };
        let value = f(receiver, sel);
        if value.is_null() {
            Err(Error::InvalidInput(
                "Objective-C message returned nil".to_owned(),
            ))
        } else {
            Ok(value)
        }
    }

    unsafe fn send_id_id(receiver: Id, sel: &str, arg: Id) -> Result<Id> {
        if receiver.is_null() {
            return Err(Error::InvalidInput(format!(
                "nil Objective-C receiver for {sel}"
            )));
        }
        let sel = unsafe { selector(sel)? };
        let f: extern "C" fn(Id, Sel, Id) -> Id =
            unsafe { mem::transmute(objc_msgSend as *const ()) };
        let value = f(receiver, sel, arg);
        if value.is_null() {
            Err(Error::InvalidInput(
                "Objective-C message returned nil".to_owned(),
            ))
        } else {
            Ok(value)
        }
    }

    unsafe fn send_id_id_id(receiver: Id, sel: &str, left: Id, right: Id) -> Result<Id> {
        if receiver.is_null() {
            return Err(Error::InvalidInput(format!(
                "nil Objective-C receiver for {sel}"
            )));
        }
        let sel = unsafe { selector(sel)? };
        let f: extern "C" fn(Id, Sel, Id, Id) -> Id =
            unsafe { mem::transmute(objc_msgSend as *const ()) };
        let value = f(receiver, sel, left, right);
        if value.is_null() {
            Err(Error::InvalidInput(
                "Objective-C message returned nil".to_owned(),
            ))
        } else {
            Ok(value)
        }
    }

    unsafe fn send_id_usize(receiver: Id, sel: &str, arg: usize) -> Result<Id> {
        if receiver.is_null() {
            return Err(Error::InvalidInput(format!(
                "nil Objective-C receiver for {sel}"
            )));
        }
        let sel = unsafe { selector(sel)? };
        let f: extern "C" fn(Id, Sel, usize) -> Id =
            unsafe { mem::transmute(objc_msgSend as *const ()) };
        let value = f(receiver, sel, arg);
        if value.is_null() {
            Err(Error::InvalidInput(
                "Objective-C message returned nil".to_owned(),
            ))
        } else {
            Ok(value)
        }
    }

    unsafe fn send_void(receiver: Id, sel: &str) -> Result<()> {
        if receiver.is_null() {
            return Err(Error::InvalidInput(format!(
                "nil Objective-C receiver for {sel}"
            )));
        }
        let sel = unsafe { selector(sel)? };
        let f: extern "C" fn(Id, Sel) = unsafe { mem::transmute(objc_msgSend as *const ()) };
        f(receiver, sel);
        Ok(())
    }

    unsafe fn send_u64(receiver: Id, sel: &str) -> Result<u64> {
        if receiver.is_null() {
            return Err(Error::InvalidInput(format!(
                "nil Objective-C receiver for {sel}"
            )));
        }
        let sel = unsafe { selector(sel)? };
        let f: extern "C" fn(Id, Sel) -> u64 = unsafe { mem::transmute(objc_msgSend as *const ()) };
        Ok(f(receiver, sel))
    }

    unsafe fn send_u32(receiver: Id, sel: &str) -> Result<u32> {
        if receiver.is_null() {
            return Err(Error::InvalidInput(format!(
                "nil Objective-C receiver for {sel}"
            )));
        }
        let sel = unsafe { selector(sel)? };
        let f: extern "C" fn(Id, Sel) -> u32 = unsafe { mem::transmute(objc_msgSend as *const ()) };
        Ok(f(receiver, sel))
    }

    unsafe fn send_i32(receiver: Id, sel: &str) -> Result<i32> {
        if receiver.is_null() {
            return Err(Error::InvalidInput(format!(
                "nil Objective-C receiver for {sel}"
            )));
        }
        let sel = unsafe { selector(sel)? };
        let f: extern "C" fn(Id, Sel) -> i32 = unsafe { mem::transmute(objc_msgSend as *const ()) };
        Ok(f(receiver, sel))
    }
}

#[cfg(not(target_os = "macos"))]
mod platform {
    use std::path::PathBuf;

    use super::XcodeMioReport;
    use crate::error::{Error, Result};
    use crate::profiler;

    pub fn decode(
        _trace_source: PathBuf,
        _profiler_directory: PathBuf,
        _stream_data_path: PathBuf,
        _profiler_summary: Option<&profiler::ProfilerStreamDataSummary>,
    ) -> Result<XcodeMioReport> {
        Err(Error::Unsupported(
            "xcode-mio is only available on macOS with Xcode installed",
        ))
    }
}
