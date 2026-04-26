use std::collections::{HashSet, VecDeque};
use std::ffi::{CStr, CString, OsStr};
use std::os::raw::{c_char, c_double, c_int, c_long, c_uint, c_void};
use std::path::Path;
use std::process::Command;
use std::thread;
use std::time::{Duration, Instant};

use tracing::{info, warn};

use crate::error::{Error, Result};

use super::{
    XCODE_APP_NAME, XcodeActionResult, XcodeAutomationStatus, XcodeButtonInfo, XcodeCheckboxInfo,
    XcodeMenuItemInfo, XcodePermissionReport, XcodeTabInfo, XcodeUiElementInfo, XcodeWindowInfo,
    XcodeWindowSnapshot, XcodeWindowStatus, is_xcode_running, open_accessibility_preferences,
    parse_status,
};

type CFTypeRef = *const c_void;
type CFStringRef = *const c_void;
type CFArrayRef = *const c_void;
type AXUIElementRef = *const c_void;
type CGEventRef = *const c_void;
type AXError = c_int;
type CFIndex = c_long;
type CFTypeID = c_long;
type Boolean = u8;

const K_CF_STRING_ENCODING_UTF8: u32 = 0x0800_0100;
const K_AX_ERROR_SUCCESS: AXError = 0;
const K_AX_ERROR_ACTION_UNSUPPORTED: AXError = -25205;
const K_AX_ERROR_API_DISABLED: AXError = -25204;
const K_AX_ERROR_ACTION_UNSUPPORTED_ALT: AXError = -25206;
const K_CF_NUMBER_SINT32_TYPE: c_int = 3;
const K_AX_VALUE_CGPOINT: c_int = 1;
const K_AX_VALUE_CGSIZE: c_int = 2;
const K_CG_EVENT_LEFT_MOUSE_DOWN: u32 = 1;
const K_CG_EVENT_LEFT_MOUSE_UP: u32 = 2;
const K_CG_EVENT_KEY_DOWN: u32 = 10;
const K_CG_EVENT_KEY_UP: u32 = 11;
const K_CG_HID_EVENT_TAP: u32 = 0;
const K_CG_EVENT_FLAG_MASK_SHIFT: u64 = 0x0002_0000;
const K_CG_EVENT_FLAG_MASK_COMMAND: u64 = 0x0010_0000;
const K_VK_G: u16 = 0x05;
const K_VK_RETURN: u16 = 0x24;
const EXPORT_SHEET_SEARCH_LIMIT: usize = 60_000;

#[repr(C)]
#[derive(Clone, Copy)]
struct CGPoint {
    x: c_double,
    y: c_double,
}

#[repr(C)]
#[derive(Clone, Copy)]
struct CGSize {
    width: c_double,
    height: c_double,
}

unsafe extern "C" {
    fn AXIsProcessTrusted() -> Boolean;
    fn AXUIElementCreateApplication(pid: c_int) -> AXUIElementRef;
    fn AXUIElementCopyAttributeValue(
        element: AXUIElementRef,
        attribute: CFStringRef,
        value: *mut CFTypeRef,
    ) -> AXError;
    fn AXUIElementSetAttributeValue(
        element: AXUIElementRef,
        attribute: CFStringRef,
        value: CFTypeRef,
    ) -> AXError;
    fn AXUIElementPerformAction(element: AXUIElementRef, action: CFStringRef) -> AXError;
    fn AXValueGetValue(value: CFTypeRef, value_type: c_int, value_ptr: *mut c_void) -> Boolean;
    fn CFStringCreateWithCString(
        alloc: CFTypeRef,
        c_str: *const c_char,
        encoding: u32,
    ) -> CFStringRef;
    fn CFStringGetCString(
        the_string: CFStringRef,
        buffer: *mut c_char,
        buffer_size: CFIndex,
        encoding: u32,
    ) -> Boolean;
    fn CFStringGetLength(the_string: CFStringRef) -> CFIndex;
    fn CFGetTypeID(cf: CFTypeRef) -> CFTypeID;
    fn CFStringGetTypeID() -> CFTypeID;
    fn CFArrayGetTypeID() -> CFTypeID;
    fn CFBooleanGetTypeID() -> CFTypeID;
    fn CFNumberGetTypeID() -> CFTypeID;
    fn CFArrayGetCount(the_array: CFArrayRef) -> CFIndex;
    fn CFArrayGetValueAtIndex(the_array: CFArrayRef, idx: CFIndex) -> CFTypeRef;
    fn CFBooleanGetValue(boolean: CFTypeRef) -> Boolean;
    fn CFNumberGetValue(number: CFTypeRef, the_type: c_int, value_ptr: *mut c_void) -> Boolean;
    fn CFRetain(cf: CFTypeRef) -> CFTypeRef;
    fn CFRelease(cf: CFTypeRef);
    fn CGEventCreateMouseEvent(
        source: CFTypeRef,
        mouse_type: u32,
        mouse_cursor_position: CGPoint,
        mouse_button: u32,
    ) -> CGEventRef;
    fn CGEventCreateKeyboardEvent(
        source: CFTypeRef,
        virtual_key: u16,
        key_down: Boolean,
    ) -> CGEventRef;
    fn CGEventSetFlags(event: CGEventRef, flags: u64);
    fn CGEventPost(tap: u32, event: CGEventRef);
}

#[link(name = "ApplicationServices", kind = "framework")]
unsafe extern "C" {}

#[derive(Debug)]
struct CfOwned {
    ptr: CFTypeRef,
}

impl CfOwned {
    fn new(ptr: CFTypeRef) -> Option<Self> {
        (!ptr.is_null()).then_some(Self { ptr })
    }

    fn ptr(&self) -> CFTypeRef {
        self.ptr
    }
}

impl Drop for CfOwned {
    fn drop(&mut self) {
        unsafe {
            CFRelease(self.ptr);
        }
    }
}

#[derive(Debug)]
struct AxElement {
    ptr: AXUIElementRef,
}

impl AxElement {
    fn new(ptr: AXUIElementRef) -> Option<Self> {
        (!ptr.is_null()).then_some(Self { ptr })
    }

    fn copy_attr(&self, name: &str) -> Option<CfOwned> {
        let key = cf_string(name)?;
        let mut value: CFTypeRef = std::ptr::null();
        let err = unsafe { AXUIElementCopyAttributeValue(self.ptr, key.ptr(), &mut value) };
        if err == K_AX_ERROR_SUCCESS {
            CfOwned::new(value)
        } else {
            None
        }
    }

    fn string_attr(&self, name: &str) -> String {
        self.copy_attr(name)
            .and_then(|value| cf_to_string(value.ptr()))
            .unwrap_or_default()
    }

    fn bool_attr(&self, name: &str) -> Option<bool> {
        self.copy_attr(name)
            .and_then(|value| cf_to_bool(value.ptr()))
    }

    fn role(&self) -> String {
        self.string_attr("AXRole")
    }

    fn subrole(&self) -> Option<String> {
        optional(self.string_attr("AXSubrole"))
    }

    fn title(&self) -> String {
        let title = self.string_attr("AXTitle");
        if title.is_empty() {
            self.string_attr("AXValue")
        } else {
            title
        }
    }

    fn label(&self) -> String {
        let title = self.title();
        if title.is_empty() {
            self.string_attr("AXDescription")
        } else {
            title
        }
    }

    fn enabled(&self) -> bool {
        self.bool_attr("AXEnabled").unwrap_or(false)
    }

    fn perform(&self, action: &str) -> AXError {
        let Some(action) = cf_string(action) else {
            return -1;
        };
        unsafe { AXUIElementPerformAction(self.ptr, action.ptr()) }
    }

    fn set_string_value(&self, value: &str) -> Result<()> {
        let key = cf_string("AXValue")
            .ok_or_else(|| Error::InvalidInput("failed to create AXValue key".to_owned()))?;
        let value = cf_string(value)
            .ok_or_else(|| Error::InvalidInput("failed to create CFString value".to_owned()))?;
        let err = unsafe { AXUIElementSetAttributeValue(self.ptr, key.ptr(), value.ptr()) };
        ax_ok(err, "set AXValue")
    }

    fn children(&self) -> Vec<AxElement> {
        let Some(value) = self.copy_attr("AXChildren") else {
            return Vec::new();
        };
        if !cf_is_type(value.ptr(), unsafe { CFArrayGetTypeID() }) {
            return Vec::new();
        }

        let count = unsafe { CFArrayGetCount(value.ptr()) };
        let mut children = Vec::with_capacity(count.max(0) as usize);
        for idx in 0..count {
            let child = unsafe { CFArrayGetValueAtIndex(value.ptr(), idx) };
            if !child.is_null() {
                let retained = unsafe { CFRetain(child) };
                if let Some(child) = AxElement::new(retained) {
                    children.push(child);
                }
            }
        }
        children
    }

    fn parent(&self) -> Option<AxElement> {
        let value = self.copy_attr("AXParent")?;
        let retained = unsafe { CFRetain(value.ptr()) };
        AxElement::new(retained)
    }

    fn point_attr(&self, name: &str) -> Option<CGPoint> {
        let value = self.copy_attr(name)?;
        let mut point = CGPoint { x: 0.0, y: 0.0 };
        let ok = unsafe {
            AXValueGetValue(
                value.ptr(),
                K_AX_VALUE_CGPOINT,
                (&mut point as *mut CGPoint).cast(),
            )
        };
        (ok != 0).then_some(point)
    }

    fn size_attr(&self, name: &str) -> Option<CGSize> {
        let value = self.copy_attr(name)?;
        let mut size = CGSize {
            width: 0.0,
            height: 0.0,
        };
        let ok = unsafe {
            AXValueGetValue(
                value.ptr(),
                K_AX_VALUE_CGSIZE,
                (&mut size as *mut CGSize).cast(),
            )
        };
        (ok != 0).then_some(size)
    }
}

impl Clone for AxElement {
    fn clone(&self) -> Self {
        Self {
            ptr: unsafe { CFRetain(self.ptr) },
        }
    }
}

impl Drop for AxElement {
    fn drop(&mut self) {
        unsafe {
            CFRelease(self.ptr);
        }
    }
}

fn ax_summary(el: &AxElement) -> String {
    format!(
        "ptr=0x{:x} role={:?} subrole={:?} title={:?} desc={:?} id={:?} value={:?} enabled={:?} focused={:?}",
        el.ptr as usize,
        el.role(),
        el.subrole(),
        el.title(),
        el.string_attr("AXDescription"),
        el.string_attr("AXIdentifier"),
        el.string_attr("AXValue"),
        el.bool_attr("AXEnabled"),
        el.bool_attr("AXFocused"),
    )
}

pub fn check_accessibility_permissions(prompt: bool) -> Result<XcodePermissionReport> {
    info!(prompt, "check accessibility permissions");
    let xcode_running = is_xcode_running()?;
    let accessibility_granted = unsafe { AXIsProcessTrusted() } != 0;
    let xcode_probe_ok = if accessibility_granted && xcode_running {
        app().map(|app| !windows(&app).is_empty()).unwrap_or(false)
    } else {
        accessibility_granted
    };
    let mut prompt_opened = false;
    if !accessibility_granted && prompt {
        open_accessibility_preferences()?;
        prompt_opened = true;
    }
    Ok(XcodePermissionReport {
        accessibility_granted,
        xcode_running,
        xcode_probe_ok,
        prompt_opened,
    })
}

pub fn activate_xcode() -> Result<()> {
    info!("activate Xcode");
    let output = Command::new("open")
        .arg("-a")
        .arg(XCODE_APP_NAME)
        .output()?;
    if output.status.success() {
        Ok(())
    } else {
        Err(Error::InvalidInput(format!(
            "failed to activate Xcode: {}",
            String::from_utf8_lossy(&output.stderr)
        )))
    }
}

pub fn list_windows() -> Result<Vec<XcodeWindowInfo>> {
    let Ok(app) = app() else {
        return Ok(Vec::new());
    };
    Ok(windows(&app).into_iter().map(window_info).collect())
}

pub fn inspect_window(trace_path: Option<&Path>) -> Result<Option<XcodeWindowSnapshot>> {
    let Ok(app) = app() else {
        return Ok(None);
    };
    let Some(window) = target_window(&app, trace_path)? else {
        return Ok(None);
    };
    let elements = descendants(&window, 5_000);
    let button_count = elements.iter().filter(|el| el.role() == "AXButton").count();
    let tab_count = elements
        .iter()
        .filter(|el| is_tab_role(&el.role(), el.subrole().as_deref()))
        .count();
    let toolbar_count = elements
        .iter()
        .filter(|el| el.role() == "AXToolbar")
        .count();
    let status = status_from_elements(&elements);
    Ok(Some(XcodeWindowSnapshot {
        window: window_info(window),
        button_count,
        tab_count,
        toolbar_count,
        status,
    }))
}

pub fn list_buttons(trace_path: Option<&Path>) -> Result<Vec<XcodeButtonInfo>> {
    let Some(window) = selected_window(trace_path)? else {
        return Ok(Vec::new());
    };
    let window_title = window.title();
    Ok(descendants(&window, 5_000)
        .into_iter()
        .filter(|el| el.role() == "AXButton")
        .map(|el| XcodeButtonInfo {
            window_title: window_title.clone(),
            name: el.title(),
            description: optional(el.string_attr("AXDescription")),
            enabled: el.enabled(),
        })
        .collect())
}

pub fn list_checkboxes(trace_path: Option<&Path>) -> Result<Vec<XcodeCheckboxInfo>> {
    let Some(window) = selected_window(trace_path)? else {
        return Ok(Vec::new());
    };
    let window_title = window.title();
    Ok(descendants(&window, 5_000)
        .into_iter()
        .filter(|el| el.role() == "AXCheckBox")
        .map(|el| XcodeCheckboxInfo {
            window_title: window_title.clone(),
            name: el.title(),
            description: optional(el.string_attr("AXDescription")),
            checked: checkbox_checked(&el),
            enabled: el.enabled(),
        })
        .collect())
}

pub fn list_tabs(trace_path: Option<&Path>) -> Result<Vec<XcodeTabInfo>> {
    let Some(window) = selected_window(trace_path)? else {
        return Ok(Vec::new());
    };
    let window_title = window.title();
    Ok(descendants(&window, 5_000)
        .into_iter()
        .filter_map(|el| {
            let role = el.role();
            let subrole = el.subrole();
            is_tab_role(&role, subrole.as_deref()).then(|| XcodeTabInfo {
                window_title: window_title.clone(),
                role,
                subrole,
                name: el.label(),
                selected: checkbox_checked(&el),
                enabled: el.enabled(),
            })
        })
        .collect())
}

pub fn list_menu_items(menu_path: &[&str]) -> Result<Vec<XcodeMenuItemInfo>> {
    let initial_app = app()?;
    let Some(current) = menu_container(&initial_app, menu_path)? else {
        return Ok(Vec::new());
    };
    let visible_path = menu_path
        .iter()
        .map(|s| (*s).to_owned())
        .collect::<Vec<_>>();
    Ok(current
        .children()
        .into_iter()
        .flat_map(menu_item_children)
        .filter(|el| is_menu_item_role(&el.role()))
        .map(|el| XcodeMenuItemInfo {
            menu_path: visible_path.clone(),
            title: el.title(),
            enabled: el.enabled(),
            has_submenu: el.children().iter().any(|child| child.role() == "AXMenu"),
        })
        .collect())
}

pub fn list_ui_elements(trace_path: Option<&Path>) -> Result<Vec<XcodeUiElementInfo>> {
    let Some(window) = selected_window(trace_path)? else {
        return Ok(Vec::new());
    };
    let mut out = Vec::new();
    collect_ui_elements(&window, Vec::new(), &mut out, 0);
    Ok(out)
}

pub fn click_button(trace_path: Option<&Path>, button_names: &[&str]) -> Result<XcodeActionResult> {
    info!(trace_path = ?trace_path, button_names = ?button_names, "AX click button: begin");
    let Some(window) = selected_window(trace_path)? else {
        warn!("AX click button: missing selected window");
        return Err(Error::InvalidInput("missing-window".to_owned()));
    };
    let window_title = window.title();
    info!(window = %ax_summary(&window), "AX click button: selected window");
    let names = button_names
        .iter()
        .map(|s| normalize(s))
        .collect::<Vec<_>>();
    let buttons = descendants(&window, 5_000)
        .into_iter()
        .filter(|el| el.role() == "AXButton")
        .collect::<Vec<_>>();
    let Some(button) = buttons.iter().find(|el| {
        names.iter().any(|name| {
            normalize(&el.label()) == *name
                || normalize(&el.title()) == *name
                || normalize(&el.string_attr("AXDescription")) == *name
        })
    }) else {
        warn!(
            requested = ?button_names,
            buttons = ?buttons
                .iter()
                .map(|el| (el.label(), el.title(), el.string_attr("AXDescription"), el.enabled()))
                .collect::<Vec<_>>(),
            "AX click button: requested button not found"
        );
        return Err(Error::InvalidInput(format!(
            "missing-action; requested={:?}; buttons={:?}",
            button_names,
            buttons
                .iter()
                .map(|el| {
                    (
                        el.label(),
                        el.title(),
                        el.string_attr("AXDescription"),
                        el.enabled(),
                    )
                })
                .collect::<Vec<_>>()
        )));
    };
    info!(button = %ax_summary(button), "AX click button: matched button");
    if !button.enabled() {
        warn!(button = %ax_summary(button), "AX click button: button disabled");
        return Err(Error::InvalidInput(format!(
            "button '{}' is disabled",
            button.label()
        )));
    }
    let target = button.label();
    info!(target, "AX click button: pressing");
    press(button, Some(&window))?;
    Ok(XcodeActionResult {
        window_title,
        action: "click-button".to_owned(),
        target,
    })
}

pub fn select_tab(trace_path: Option<&Path>, tab_name: &str) -> Result<XcodeActionResult> {
    info!(trace_path = ?trace_path, tab_name, "AX select tab: begin");
    let Some(window) = selected_window(trace_path)? else {
        warn!("AX select tab: missing selected window");
        return Err(Error::InvalidInput("missing-window".to_owned()));
    };
    let window_title = window.title();
    info!(window = %ax_summary(&window), "AX select tab: selected window");
    let name = normalize(tab_name);
    let elements = descendants(&window, 5_000);
    if let Some(tab) = elements.iter().find(|el| {
        is_tab_role(&el.role(), el.subrole().as_deref()) && normalize(&el.label()) == name
    }) {
        info!(tab = %ax_summary(tab), "AX select tab: pressing tab element");
        press(tab, Some(&window))?;
        return Ok(XcodeActionResult {
            window_title,
            action: "select-tab".to_owned(),
            target: tab_name.to_owned(),
        });
    }

    let Some(item) = elements.iter().find(|el| {
        matches!(
            el.role().as_str(),
            "AXStaticText" | "AXCell" | "AXRow" | "AXButton"
        ) && normalize(&el.label()) == name
    }) else {
        warn!(tab_name, "AX select tab: tab/action element not found");
        return Err(Error::InvalidInput("missing-action".to_owned()));
    };
    let target = clickable_navigation_ancestor(item).unwrap_or_else(|| item.clone());
    info!(
        item = %ax_summary(item),
        target = %ax_summary(&target),
        "AX select tab: pressing navigation ancestor"
    );
    press(&target, Some(&window))?;
    Ok(XcodeActionResult {
        window_title,
        action: "select-tab".to_owned(),
        target: tab_name.to_owned(),
    })
}

pub fn click_menu_item(menu_path: &[&str]) -> Result<XcodeActionResult> {
    info!(menu_path = ?menu_path, "AX click menu item: begin");
    let app = app()?;
    if menu_path.is_empty() {
        warn!("AX click menu item: empty menu path");
        return Err(Error::InvalidInput("missing menu path".to_owned()));
    }
    let mut current = menu_bar(&app)?;
    let mut target = String::new();
    for (idx, segment) in menu_path.iter().enumerate() {
        info!(idx, segment, current = %ax_summary(&current), "AX click menu item: resolve segment");
        let Some(item) = find_menu_child(&current, segment) else {
            warn!(idx, segment, "AX click menu item: segment not found");
            return Err(Error::InvalidInput(format!(
                "menu item '{}' not found",
                segment
            )));
        };
        target = item.title();
        info!(idx, segment, item = %ax_summary(&item), "AX click menu item: pressing segment");
        press(&item, None)?;
        thread::sleep(Duration::from_millis(if idx + 1 == menu_path.len() {
            150
        } else {
            250
        }));
        current = item;
    }
    Ok(XcodeActionResult {
        window_title: XCODE_APP_NAME.to_owned(),
        action: "click-menu-item".to_owned(),
        target,
    })
}

pub fn close_window(trace_path: Option<&Path>) -> Result<XcodeActionResult> {
    let Some(window) = selected_window(trace_path)? else {
        return Err(Error::InvalidInput("missing-window".to_owned()));
    };
    let window_title = window.title();
    if let Some(close_button) = find_descendant(&window, 200, |el| {
        el.role() == "AXButton" && normalize(&el.string_attr("AXSubrole")).contains("close")
    }) {
        press(&close_button, Some(&window))?;
    } else {
        ax_ok(window.perform("AXCancel"), "close window")?;
    }
    Ok(XcodeActionResult {
        window_title,
        action: "close-window".to_owned(),
        target: "window".to_owned(),
    })
}

pub fn ensure_checked(trace_path: Option<&Path>, checkbox_name: &str) -> Result<XcodeActionResult> {
    checkbox_action(trace_path, checkbox_name, false)
}

pub fn toggle_checkbox(
    trace_path: Option<&Path>,
    checkbox_name: &str,
) -> Result<XcodeActionResult> {
    checkbox_action(trace_path, checkbox_name, true)
}

pub fn get_window_status(trace_path: Option<&Path>) -> Result<XcodeWindowStatus> {
    let Some(window) = selected_window(trace_path)? else {
        return Ok(XcodeWindowStatus {
            status: XcodeAutomationStatus::NotRunning,
            raw: "not-running".to_owned(),
            current_tab: None,
            available_actions: Vec::new(),
        });
    };
    let elements = descendants(&window, 5_000);
    let status = status_from_elements(&elements);
    let raw = raw_status(&status);
    let current_tab = elements
        .iter()
        .find(|el| is_tab_role(&el.role(), el.subrole().as_deref()) && checkbox_checked(el))
        .map(AxElement::label);
    let mut available_actions = elements
        .iter()
        .filter(|el| el.role() == "AXButton" && el.enabled())
        .map(AxElement::label)
        .filter(|name| {
            matches!(
                name.as_str(),
                "Replay" | "Profile" | "Capture GPU workload" | "Show Performance" | "Export"
            )
        })
        .collect::<Vec<_>>();
    available_actions.sort();
    available_actions.dedup();
    Ok(XcodeWindowStatus {
        status,
        raw,
        current_tab,
        available_actions,
    })
}

pub fn dismiss_startup_dialogs() -> Result<bool> {
    let Ok(app) = app() else {
        return Ok(false);
    };
    for window in windows(&app) {
        for name in ["Reopen", "Continue", "Open"] {
            if let Some(button) = find_button(&window, name, 500)
                && button.enabled()
            {
                press(&button, Some(&window))?;
                return Ok(true);
            }
        }
    }
    Ok(false)
}

pub fn running_profile_window() -> Result<Option<String>> {
    let Ok(app) = app() else {
        return Ok(None);
    };
    for window in windows(&app) {
        let elements = descendants(&window, 2_000);
        let has_show_performance = elements
            .iter()
            .any(|el| el.role() == "AXButton" && el.label() == "Show Performance");
        let running_stop = elements.iter().any(|el| {
            el.role() == "AXButton"
                && el.enabled()
                && (el.label() == "Stop GPU workload" || el.label().starts_with("Stop GPU"))
        });
        if running_stop && !has_show_performance {
            return Ok(Some(window.title()));
        }
    }
    Ok(None)
}

pub fn finish_export_sheet(
    parent: &Path,
    file_name: &OsStr,
    trace_path: Option<&Path>,
) -> Result<XcodeActionResult> {
    info!(
        parent = %parent.display(),
        file_name = ?file_name,
        trace_path = ?trace_path,
        "AX finish export sheet: begin"
    );
    let initial_app = app()?;
    let deadline = Instant::now() + Duration::from_secs(15);
    let mut sheet_target = None;
    let mut poll_count = 0usize;
    while Instant::now() < deadline {
        poll_count += 1;
        let fresh_app = app().unwrap_or_else(|_| initial_app.clone());
        if let Some(target) = find_export_sheet(&fresh_app) {
            info!(
                poll_count,
                "AX finish export sheet: found export sheet from app windows"
            );
            sheet_target = Some(target);
            break;
        }
        thread::sleep(Duration::from_millis(250));
    }
    let (window, sheet) = if let Some(target) = sheet_target {
        target
    } else {
        warn!(
            poll_count,
            "AX finish export sheet: polling did not find sheet, falling back to selected window"
        );
        let Some(window) = selected_window(trace_path)? else {
            warn!("AX finish export sheet: selected window missing");
            return Err(Error::InvalidInput(
                "export sheet did not appear".to_owned(),
            ));
        };
        info!(window = %ax_summary(&window), "AX finish export sheet: selected window fallback");
        let Some(sheet) = find_export_sheet_in_window(&window).or_else(|| {
            find_save_button(&window, EXPORT_SHEET_SEARCH_LIMIT).map(|_| window.clone())
        }) else {
            warn!("AX finish export sheet: export sheet missing in selected window");
            return Err(Error::InvalidInput(
                "export sheet did not appear".to_owned(),
            ));
        };
        (window, sheet)
    };
    info!(
        window = %ax_summary(&window),
        sheet = %ax_summary(&sheet),
        "AX finish export sheet: target sheet resolved"
    );

    info!("AX finish export sheet: navigate to requested parent");
    navigate_to_parent(&window, &sheet, parent)?;
    info!("AX finish export sheet: wait for export sheet after path navigation");
    let sheet = match wait_for_export_sheet(&window, trace_path, Duration::from_secs(8)) {
        Some(sheet) => {
            info!(sheet = %ax_summary(&sheet), "AX finish export sheet: reacquired export sheet");
            sheet
        }
        None => {
            warn!(
                "AX finish export sheet: failed to reacquire export sheet, using previous sheet handle"
            );
            sheet
        }
    };

    info!("AX finish export sheet: ensure embed performance data");
    ensure_export_checkbox(&window, &sheet, "Embed performance data")?;

    let output_name = file_name.to_string_lossy();
    if let Some(field) = find_save_as_field(&sheet) {
        info!(
            field = %ax_summary(&field),
            output_name = %output_name,
            "AX finish export sheet: set Save As field"
        );
        field.set_string_value(&output_name)?;
        let actual = field.string_attr("AXValue");
        info!(
            expected = %output_name,
            actual = %actual,
            matches = actual == output_name,
            "AX finish export sheet: Save As AXValue after set"
        );
        if actual != output_name {
            return Err(Error::InvalidInput(format!(
                "Save As field value mismatch: expected {output_name:?}, got {actual:?}"
            )));
        }
    } else {
        warn!("AX finish export sheet: Save As field not found");
    }

    info!("AX finish export sheet: check replace dialog before save");
    if press_replace_if_present(&window) {
        info!("AX finish export sheet: replace dialog handled before save");
        return Ok(XcodeActionResult {
            window_title: window.title(),
            action: "save-export".to_owned(),
            target: output_name.into_owned(),
        });
    }

    let Some(button) = find_save_button(&sheet, EXPORT_SHEET_SEARCH_LIMIT) else {
        warn!(
            sheet = %ax_summary(&sheet),
            "AX finish export sheet: Save/Export button not found"
        );
        return Err(Error::InvalidInput(
            "Save/Export button not found".to_owned(),
        ));
    };
    info!(button = %ax_summary(&button), "AX finish export sheet: Save/Export button found");
    if !button.enabled() {
        warn!(button = %ax_summary(&button), "AX finish export sheet: Save/Export button disabled");
        return Err(Error::InvalidInput(
            "Save/Export button is disabled in export sheet".to_owned(),
        ));
    }
    info!("AX finish export sheet: press Save/Export button");
    press(&button, Some(&window))?;
    thread::sleep(Duration::from_millis(500));
    let replaced = press_replace_if_present(&window);
    info!(
        replaced,
        "AX finish export sheet: replace dialog check after save"
    );
    Ok(XcodeActionResult {
        window_title: window.title(),
        action: "save-export".to_owned(),
        target: output_name.into_owned(),
    })
}

fn find_export_sheet(app: &AxElement) -> Option<(AxElement, AxElement)> {
    let windows = windows(app);
    info!(
        window_count = windows.len(),
        "AX find export sheet: scanning windows"
    );
    windows.into_iter().find_map(|window| {
        let found = find_export_sheet_in_window(&window);
        if let Some(sheet) = &found {
            info!(
                window = %ax_summary(&window),
                sheet = %ax_summary(sheet),
                "AX find export sheet: found"
            );
        }
        found.map(|sheet| (window, sheet))
    })
}

fn find_export_sheet_in_window(window: &AxElement) -> Option<AxElement> {
    let found = find_descendant(window, EXPORT_SHEET_SEARCH_LIMIT, |el| {
        el.role() == "AXSheet" && !is_go_to_folder_sheet(el)
    });
    if let Some(sheet) = &found {
        info!(
            window = %ax_summary(window),
            sheet = %ax_summary(sheet),
            "AX find export sheet in window: found"
        );
    }
    found
}

fn is_go_to_folder_sheet(sheet: &AxElement) -> bool {
    sheet.role() == "AXSheet" && normalize(&sheet.string_attr("AXIdentifier")) == "gotowindow"
}

fn app() -> Result<AxElement> {
    let pid = xcode_pid()?;
    let ptr = unsafe { AXUIElementCreateApplication(pid) };
    AxElement::new(ptr)
        .ok_or_else(|| Error::InvalidInput("failed to create Xcode AX object".into()))
}

fn xcode_pid() -> Result<c_int> {
    let output = Command::new("pgrep")
        .arg("-x")
        .arg(XCODE_APP_NAME)
        .output()?;
    if !output.status.success() {
        return Err(Error::InvalidInput("Xcode not running".to_owned()));
    }
    let stdout = String::from_utf8_lossy(&output.stdout);
    stdout
        .lines()
        .next()
        .and_then(|line| line.trim().parse::<c_int>().ok())
        .ok_or_else(|| Error::InvalidInput("failed to parse Xcode pid".to_owned()))
}

fn selected_window(trace_path: Option<&Path>) -> Result<Option<AxElement>> {
    info!(trace_path = ?trace_path, "AX selected window: begin");
    let Ok(app) = app() else {
        warn!("AX selected window: app unavailable");
        return Ok(None);
    };
    let selected = target_window(&app, trace_path)?;
    if let Some(window) = &selected {
        info!(window = %ax_summary(window), "AX selected window: found");
    } else {
        warn!("AX selected window: none");
    }
    Ok(selected)
}

fn target_window(app: &AxElement, trace_path: Option<&Path>) -> Result<Option<AxElement>> {
    let all = windows(app);
    info!(
        window_count = all.len(),
        patterns = ?trace_patterns(trace_path),
        "AX target window: begin"
    );
    if all.is_empty() {
        return Ok(None);
    }
    let patterns = trace_patterns(trace_path);
    if !patterns.is_empty() {
        let matches = all
            .iter()
            .filter(|window| {
                let title = normalize(&window.title());
                let document = normalize(&window.string_attr("AXDocument"));
                patterns
                    .iter()
                    .any(|pattern| title.contains(pattern) || document.contains(pattern))
            })
            .cloned()
            .collect::<Vec<_>>();
        if let Some(preferred) = preferred_trace_window(matches) {
            info!(window = %ax_summary(&preferred), "AX target window: selected by trace path pattern");
            return Ok(Some(preferred));
        }
    }
    if let Some(trace_window) = preferred_trace_window(
        all.iter()
            .filter(|window| {
                window.title().ends_with(".gputrace")
                    || window.string_attr("AXDocument").contains(".gputrace")
                    || has_trace_landmark(window)
            })
            .cloned()
            .collect(),
    ) {
        info!(window = %ax_summary(&trace_window), "AX target window: selected by trace landmark");
        return Ok(Some(trace_window));
    }
    let fallback = all.into_iter().next();
    if let Some(window) = &fallback {
        info!(window = %ax_summary(window), "AX target window: selected fallback first window");
    }
    Ok(fallback)
}

fn windows(app: &AxElement) -> Vec<AxElement> {
    if let Some(value) = app.copy_attr("AXWindows")
        && cf_is_type(value.ptr(), unsafe { CFArrayGetTypeID() })
    {
        let count = unsafe { CFArrayGetCount(value.ptr()) };
        let mut out = Vec::new();
        for idx in 0..count {
            let child = unsafe { CFArrayGetValueAtIndex(value.ptr(), idx) };
            if !child.is_null() {
                let retained = unsafe { CFRetain(child) };
                if let Some(window) = AxElement::new(retained)
                    && window.role() == "AXWindow"
                {
                    out.push(window);
                }
            }
        }
        return out;
    }
    app.children()
        .into_iter()
        .filter(|child| child.role() == "AXWindow")
        .collect()
}

fn preferred_trace_window(candidates: Vec<AxElement>) -> Option<AxElement> {
    if candidates.is_empty() {
        return None;
    }
    for name in ["Replay", "Export", "Show Performance"] {
        if let Some(window) = candidates
            .iter()
            .find(|window| find_button(window, name, 1_500).is_some())
        {
            return Some(window.clone());
        }
    }
    candidates.into_iter().next()
}

fn has_trace_landmark(window: &AxElement) -> bool {
    ["Replay", "Profile", "Export", "Show Performance"]
        .iter()
        .any(|name| find_button(window, name, 1_000).is_some())
}

fn trace_patterns(trace_path: Option<&Path>) -> Vec<String> {
    let Some(path) = trace_path else {
        return Vec::new();
    };
    let mut out = Vec::new();
    if let Some(file) = path.file_name().and_then(OsStr::to_str) {
        out.push(normalize(file));
    }
    if let Some(stem) = path.file_stem().and_then(OsStr::to_str) {
        let stem = normalize(stem);
        if !out.contains(&stem) {
            out.push(stem);
        }
    }
    out
}

fn window_info(window: AxElement) -> XcodeWindowInfo {
    XcodeWindowInfo {
        title: window.title(),
        document: optional(window.string_attr("AXDocument")),
        role: window.role(),
        subrole: window.subrole(),
        focused: window.bool_attr("AXFocused").unwrap_or(false),
        main: window.bool_attr("AXMain").unwrap_or(false),
        modal: window.bool_attr("AXModal").unwrap_or(false),
    }
}

fn descendants(root: &AxElement, max_visit: usize) -> Vec<AxElement> {
    let mut out = Vec::new();
    let mut queue = VecDeque::from([root.clone()]);
    let mut seen = HashSet::new();
    while let Some(el) = queue.pop_front() {
        if out.len() >= max_visit {
            break;
        }
        let key = el.ptr as usize;
        if !seen.insert(key) {
            continue;
        }
        for child in el.children() {
            queue.push_back(child);
        }
        out.push(el);
    }
    out
}

fn find_descendant(
    root: &AxElement,
    max_visit: usize,
    mut pred: impl FnMut(&AxElement) -> bool,
) -> Option<AxElement> {
    let mut queue = VecDeque::from([root.clone()]);
    let mut seen = HashSet::new();
    let mut visited = 0;
    while let Some(el) = queue.pop_front() {
        if visited >= max_visit {
            break;
        }
        let key = el.ptr as usize;
        if !seen.insert(key) {
            continue;
        }
        visited += 1;
        if pred(&el) {
            return Some(el);
        }
        for child in el.children() {
            queue.push_back(child);
        }
    }
    None
}

fn find_button(root: &AxElement, name: &str, max_visit: usize) -> Option<AxElement> {
    let name = normalize(name);
    find_descendant(root, max_visit, |el| {
        el.role() == "AXButton"
            && (normalize(&el.title()) == name
                || normalize(&el.string_attr("AXDescription")) == name)
    })
}

fn find_checkbox(root: &AxElement, name: &str, max_visit: usize) -> Option<AxElement> {
    let name = normalize(name);
    find_descendant(root, max_visit, |el| {
        el.role() == "AXCheckBox"
            && (normalize(&el.title()) == name
                || normalize(&el.string_attr("AXDescription")) == name)
    })
}

fn find_save_button(root: &AxElement, max_visit: usize) -> Option<AxElement> {
    for name in ["Save", "Export"] {
        let button = find_button(root, name, max_visit);
        match &button {
            Some(button) => info!(
                name,
                button = %ax_summary(button),
                "AX find save button: candidate"
            ),
            None => info!(name, "AX find save button: candidate missing"),
        }
        if let Some(button) = button.filter(AxElement::enabled) {
            info!(name, "AX find save button: using enabled candidate");
            return Some(button);
        }
    }
    None
}

fn ensure_export_checkbox(window: &AxElement, sheet: &AxElement, name: &str) -> Result<()> {
    info!(name, "AX ensure export checkbox: begin");
    let Some(embed) = find_checkbox(sheet, name, EXPORT_SHEET_SEARCH_LIMIT) else {
        warn!(name, "AX ensure export checkbox: checkbox not found");
        return Ok(());
    };
    info!(
        checkbox = %ax_summary(&embed),
        checked = checkbox_checked(&embed),
        name,
        "AX ensure export checkbox: checkbox found"
    );
    if !embed.enabled() {
        warn!(checkbox = %ax_summary(&embed), name, "AX ensure export checkbox: checkbox disabled");
        return Ok(());
    }
    if !checkbox_checked(&embed) {
        info!(name, "AX ensure export checkbox: pressing checkbox");
        press(&embed, Some(window))?;
        thread::sleep(Duration::from_millis(300));
        let checked = checkbox_checked(&embed);
        info!(checked, checkbox = %ax_summary(&embed), name, "AX ensure export checkbox: after press");
        if !checked {
            return Err(Error::InvalidInput(format!(
                "export checkbox did not become checked: {name}"
            )));
        }
    } else {
        info!(name, "AX ensure export checkbox: already checked");
    }
    Ok(())
}

fn find_save_as_field(root: &AxElement) -> Option<AxElement> {
    find_descendant(root, EXPORT_SHEET_SEARCH_LIMIT, |el| {
        matches!(el.role().as_str(), "AXTextField" | "AXComboBox")
            && el.string_attr("AXIdentifier") == "saveAsNameTextField"
    })
    .or_else(|| {
        find_descendant(root, EXPORT_SHEET_SEARCH_LIMIT, |el| {
            el.role() == "AXTextField"
                && normalize(&el.string_attr("AXDescription")).contains("save")
        })
    })
}

fn navigate_to_parent(window: &AxElement, sheet: &AxElement, parent: &Path) -> Result<()> {
    info!(
        parent = %parent.display(),
        window = %ax_summary(window),
        sheet = %ax_summary(sheet),
        "AX navigate to parent: begin"
    );
    std::fs::create_dir_all(parent)?;
    let parent = parent.to_string_lossy();
    if let Some(path_field) = find_descendant(sheet, EXPORT_SHEET_SEARCH_LIMIT, |el| {
        matches!(el.role().as_str(), "AXTextField" | "AXComboBox")
            && (el.string_attr("AXIdentifier") == "PathTextField"
                || normalize(&el.string_attr("AXDescription")).contains("path")
                || normalize(&el.string_attr("AXDescription")).contains("folder"))
    }) {
        info!(
            field = %ax_summary(&path_field),
            parent = %parent,
            "AX navigate to parent: using direct path field"
        );
        path_field.set_string_value(&parent)?;
        info!(
            value = %path_field.string_attr("AXValue"),
            "AX navigate to parent: direct path field AXValue after set"
        );
        confirm_text_entry(window, &path_field)?;
        return Ok(());
    }
    info!("AX navigate to parent: direct path field not found");

    match activate_xcode() {
        Ok(()) => info!("AX navigate to parent: activated Xcode"),
        Err(error) => warn!(%error, "AX navigate to parent: activate Xcode failed"),
    }
    let raise_err = window.perform("AXRaise");
    info!(raise_err, "AX navigate to parent: raised window");

    let go_field = if let Some(field) = find_go_to_folder_field(window, sheet) {
        info!(field = %ax_summary(&field), "AX navigate to parent: existing Go To field found");
        field
    } else {
        info!("AX navigate to parent: posting Cmd+Shift+G");
        post_key(
            K_VK_G,
            K_CG_EVENT_FLAG_MASK_COMMAND | K_CG_EVENT_FLAG_MASK_SHIFT,
        )?;
        thread::sleep(Duration::from_millis(700));
        find_go_to_folder_field(window, sheet)
            .ok_or_else(|| Error::InvalidInput("Go To Folder field not found".to_owned()))?
    };

    if !is_go_to_folder_text_field(&go_field) {
        warn!(field = %ax_summary(&go_field), "AX navigate to parent: Go To field predicate failed");
        return Err(Error::InvalidInput(
            "Go To Folder field not found".to_owned(),
        ));
    };

    info!(
        field = %ax_summary(&go_field),
        parent = %parent,
        "AX navigate to parent: set Go To Folder field"
    );
    go_field.set_string_value(&parent)?;
    let actual = go_field.string_attr("AXValue");
    info!(
        expected = %parent,
        actual = %actual,
        matches = actual == parent,
        "AX navigate to parent: Go To Folder AXValue after set"
    );
    if actual != parent {
        return Err(Error::InvalidInput(format!(
            "Go To Folder field value mismatch: expected {parent:?}, got {actual:?}"
        )));
    }
    confirm_go_to_folder_entry(window, &go_field)?;
    Ok(())
}

fn find_go_to_folder_field(window: &AxElement, sheet: &AxElement) -> Option<AxElement> {
    info!("AX find Go To Folder field: begin");
    let go_sheet = find_descendant(window, EXPORT_SHEET_SEARCH_LIMIT, is_go_to_folder_sheet)
        .or_else(|| find_descendant(sheet, EXPORT_SHEET_SEARCH_LIMIT, is_go_to_folder_sheet));
    let Some(go_sheet) = go_sheet else {
        info!("AX find Go To Folder field: sheet not found");
        return None;
    };
    info!(sheet = %ax_summary(&go_sheet), "AX find Go To Folder field: sheet found");
    find_descendant(
        &go_sheet,
        EXPORT_SHEET_SEARCH_LIMIT,
        is_go_to_folder_text_field,
    )
    .inspect(|field| {
        info!(field = %ax_summary(field), "AX find Go To Folder field: field found");
    })
}

fn is_go_to_folder_text_field(el: &AxElement) -> bool {
    matches!(
        el.role().as_str(),
        "AXTextField" | "AXComboBox" | "AXTextArea"
    )
}

fn confirm_text_entry(window: &AxElement, field: &AxElement) -> Result<()> {
    info!(
        field = %ax_summary(field),
        "AX confirm text entry: begin"
    );
    match activate_xcode() {
        Ok(()) => info!("AX confirm text entry: activated Xcode"),
        Err(error) => warn!(%error, "AX confirm text entry: activate Xcode failed"),
    }
    let raise_err = window.perform("AXRaise");
    info!(raise_err, "AX confirm text entry: raised window");
    thread::sleep(Duration::from_millis(150));
    info!("AX confirm text entry: posting Return");
    post_key(K_VK_RETURN, 0)?;
    thread::sleep(Duration::from_millis(250));
    Ok(())
}

fn confirm_go_to_folder_entry(window: &AxElement, field: &AxElement) -> Result<()> {
    info!(
        field = %ax_summary(field),
        value = %field.string_attr("AXValue"),
        "AX confirm Go To Folder: begin"
    );
    match activate_xcode() {
        Ok(()) => info!("AX confirm Go To Folder: activated Xcode"),
        Err(error) => warn!(%error, "AX confirm Go To Folder: activate Xcode failed"),
    }
    let raise_err = window.perform("AXRaise");
    info!(raise_err, "AX confirm Go To Folder: raised window");
    thread::sleep(Duration::from_millis(150));

    info!("AX confirm Go To Folder: posting Return");
    post_key(K_VK_RETURN, 0)?;
    if wait_for_go_to_folder_sheet_closed(window, Duration::from_secs(2)) {
        info!("AX confirm Go To Folder: closed after Return");
        return Ok(());
    }

    warn!(
        field = %ax_summary(field),
        value = %field.string_attr("AXValue"),
        "AX confirm Go To Folder: sheet stayed open after Return"
    );
    Err(Error::InvalidInput(
        "Go To Folder sheet stayed open after pressing Return".to_owned(),
    ))
}

fn wait_for_go_to_folder_sheet_closed(window: &AxElement, timeout: Duration) -> bool {
    info!(
        timeout_ms = timeout.as_millis(),
        "AX wait Go To Folder sheet closed: begin"
    );
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        if find_descendant(window, EXPORT_SHEET_SEARCH_LIMIT, is_go_to_folder_sheet).is_none() {
            info!("AX wait Go To Folder sheet closed: closed");
            return true;
        }
        thread::sleep(Duration::from_millis(100));
    }
    warn!("AX wait Go To Folder sheet closed: timeout");
    false
}

fn close_go_to_folder_sheet_if_present(window: &AxElement) {
    info!("AX close Go To Folder sheet if present: begin");
    let Some(sheet) = find_descendant(window, EXPORT_SHEET_SEARCH_LIMIT, is_go_to_folder_sheet)
    else {
        info!("AX close Go To Folder sheet if present: no sheet");
        return;
    };
    info!(sheet = %ax_summary(&sheet), "AX close Go To Folder sheet if present: sheet found");
    if let Some(close) = find_button(&sheet, "Close", EXPORT_SHEET_SEARCH_LIMIT) {
        info!(button = %ax_summary(&close), "AX close Go To Folder sheet if present: pressing Close");
        match press(&close, Some(window)) {
            Ok(()) => info!("AX close Go To Folder sheet if present: Close pressed"),
            Err(error) => warn!(%error, "AX close Go To Folder sheet if present: Close failed"),
        }
    } else {
        let err = sheet.perform("AXCancel");
        info!(err, "AX close Go To Folder sheet if present: AXCancel");
    }
    thread::sleep(Duration::from_millis(300));
}

fn press_replace_if_present(window: &AxElement) -> bool {
    info!("AX press Replace if present: begin");
    let Some(replace) =
        find_button(window, "Replace", EXPORT_SHEET_SEARCH_LIMIT).filter(AxElement::enabled)
    else {
        info!("AX press Replace if present: no enabled Replace button");
        return false;
    };
    info!(button = %ax_summary(&replace), "AX press Replace if present: pressing Replace");
    match press(&replace, Some(window)) {
        Ok(()) => {
            info!("AX press Replace if present: success");
            true
        }
        Err(error) => {
            warn!(%error, "AX press Replace if present: failed");
            false
        }
    }
}

fn wait_for_export_sheet_with_save_button(
    window: &AxElement,
    trace_path: Option<&Path>,
    timeout: Duration,
) -> Option<AxElement> {
    info!(
        timeout_ms = timeout.as_millis(),
        "AX wait export sheet with save button: begin"
    );
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        if let Some(sheet) = find_export_sheet_in_window(window)
            && find_save_button(&sheet, EXPORT_SHEET_SEARCH_LIMIT).is_some()
        {
            info!(sheet = %ax_summary(&sheet), "AX wait export sheet with save button: found in provided window");
            return Some(sheet);
        }
        if let Ok(Some(selected)) = selected_window(trace_path)
            && let Some(sheet) = find_export_sheet_in_window(&selected)
            && find_save_button(&sheet, EXPORT_SHEET_SEARCH_LIMIT).is_some()
        {
            info!(sheet = %ax_summary(&sheet), "AX wait export sheet with save button: found in selected window");
            return Some(sheet);
        }
        thread::sleep(Duration::from_millis(150));
    }
    warn!("AX wait export sheet with save button: timeout");
    None
}

fn wait_for_export_sheet(
    window: &AxElement,
    trace_path: Option<&Path>,
    timeout: Duration,
) -> Option<AxElement> {
    info!(
        timeout_ms = timeout.as_millis(),
        "AX wait export sheet: begin"
    );
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        if let Some(sheet) = find_export_sheet_in_window(window) {
            info!(sheet = %ax_summary(&sheet), "AX wait export sheet: found in provided window");
            return Some(sheet);
        }
        if let Ok(Some(selected)) = selected_window(trace_path)
            && let Some(sheet) = find_export_sheet_in_window(&selected)
        {
            info!(sheet = %ax_summary(&sheet), "AX wait export sheet: found in selected window");
            return Some(sheet);
        }
        thread::sleep(Duration::from_millis(150));
    }
    warn!("AX wait export sheet: timeout");
    None
}

fn checkbox_action(
    trace_path: Option<&Path>,
    checkbox_name: &str,
    toggle: bool,
) -> Result<XcodeActionResult> {
    info!(trace_path = ?trace_path, checkbox_name, toggle, "AX checkbox action: begin");
    let Some(window) = selected_window(trace_path)? else {
        warn!("AX checkbox action: missing selected window");
        return Err(Error::InvalidInput("missing-window".to_owned()));
    };
    let Some(checkbox) = find_checkbox(&window, checkbox_name, 5_000) else {
        warn!(checkbox_name, "AX checkbox action: checkbox not found");
        return Err(Error::InvalidInput("missing-action".to_owned()));
    };
    let checked = checkbox_checked(&checkbox);
    info!(checkbox = %ax_summary(&checkbox), checked, "AX checkbox action: checkbox found");
    if toggle || !checked {
        info!("AX checkbox action: pressing checkbox");
        press(&checkbox, Some(&window))?;
    } else {
        info!("AX checkbox action: checkbox already checked");
    }
    Ok(XcodeActionResult {
        window_title: window.title(),
        action: if toggle {
            "toggle-checkbox"
        } else {
            "ensure-checked"
        }
        .to_owned(),
        target: checkbox_name.to_owned(),
    })
}

fn checkbox_checked(el: &AxElement) -> bool {
    el.copy_attr("AXValue")
        .and_then(|value| cf_to_bool(value.ptr()))
        .unwrap_or(false)
}

fn clickable_navigation_ancestor(el: &AxElement) -> Option<AxElement> {
    let mut current = el.clone();
    for _ in 0..8 {
        if matches!(current.role().as_str(), "AXRow" | "AXCell" | "AXButton") {
            return Some(current);
        }
        current = current.parent()?;
    }
    None
}

fn status_from_elements(elements: &[AxElement]) -> XcodeAutomationStatus {
    let has_complete_landmark = elements.iter().any(|el| {
        let text = el.label();
        (el.role() == "AXButton" && matches!(text.as_str(), "Show Performance" | "Export"))
            || matches!(
                text.as_str(),
                "Effective GPU Time"
                    | "Top Shaders"
                    | "GPU Commands"
                    | "Performance State"
                    | "Overview"
                    | "Timeline"
                    | "Shaders"
                    | "Counters"
                    | "Cost Graph"
                    | "Heat Map"
            )
            || text.ends_with(" ms")
    });

    for el in elements {
        let text = el.label();
        if text.contains("Profiling GPU Trace") {
            return XcodeAutomationStatus::Running;
        }
    }

    if has_complete_landmark {
        return XcodeAutomationStatus::Complete;
    }

    for el in elements {
        let text = el.label();
        if text.contains("Performance data not available") {
            return XcodeAutomationStatus::ReplayReady;
        }
    }
    for el in elements {
        if el.role() == "AXButton" {
            let label = el.label();
            if (label == "Stop" || label == "Stop GPU workload" || label.starts_with("Stop GPU"))
                && el.enabled()
            {
                return XcodeAutomationStatus::Running;
            }
            if label == "Profile" || label == "Replay" {
                return if el.enabled() {
                    XcodeAutomationStatus::ReplayReady
                } else {
                    XcodeAutomationStatus::Initializing
                };
            }
        }
    }
    parse_status("unknown")
}

fn raw_status(status: &XcodeAutomationStatus) -> String {
    match status {
        XcodeAutomationStatus::NotRunning => "not-running",
        XcodeAutomationStatus::Initializing => "initializing",
        XcodeAutomationStatus::ReplayReady => "replay-ready",
        XcodeAutomationStatus::Running => "running",
        XcodeAutomationStatus::Complete => "complete",
        XcodeAutomationStatus::Unknown => "unknown",
    }
    .to_owned()
}

fn collect_ui_elements(
    el: &AxElement,
    path: Vec<String>,
    out: &mut Vec<XcodeUiElementInfo>,
    depth: usize,
) {
    if out.len() > 5_000 || depth > 80 {
        return;
    }
    let label = el.label();
    let mut next_path = path;
    next_path.push(if label.is_empty() {
        el.role()
    } else {
        format!("{}:{label}", el.role())
    });
    out.push(XcodeUiElementInfo {
        path: next_path.clone(),
        role: el.role(),
        title: optional(el.title()),
        description: optional(el.string_attr("AXDescription")),
        identifier: optional(el.string_attr("AXIdentifier")),
        enabled: el.bool_attr("AXEnabled"),
    });
    for child in el.children() {
        collect_ui_elements(&child, next_path.clone(), out, depth + 1);
    }
}

fn menu_bar(app: &AxElement) -> Result<AxElement> {
    find_descendant(app, 1_000, |el| el.role() == "AXMenuBar")
        .ok_or_else(|| Error::InvalidInput("menubar not found".to_owned()))
}

fn menu_container(app: &AxElement, menu_path: &[&str]) -> Result<Option<AxElement>> {
    let mut current = menu_bar(app)?;
    if menu_path.is_empty() {
        return Ok(Some(current));
    }
    for segment in menu_path {
        let Some(item) = find_menu_child(&current, segment) else {
            return Ok(None);
        };
        press(&item, None)?;
        thread::sleep(Duration::from_millis(150));
        current = item;
    }
    Ok(Some(current))
}

fn find_menu_child(root: &AxElement, name: &str) -> Option<AxElement> {
    let name = normalize_menu(name);
    find_descendant(root, 500, |el| {
        is_menu_item_role(&el.role()) && normalize_menu(&el.title()) == name
    })
}

fn menu_item_children(el: AxElement) -> Vec<AxElement> {
    let children = el.children();
    if el.role() == "AXMenu" {
        children
    } else {
        children
            .into_iter()
            .flat_map(|child| {
                if child.role() == "AXMenu" {
                    child.children()
                } else {
                    vec![child]
                }
            })
            .collect()
    }
}

fn is_menu_item_role(role: &str) -> bool {
    matches!(role, "AXMenuBarItem" | "AXMenuItem")
}

fn is_tab_role(role: &str, subrole: Option<&str>) -> bool {
    role == "AXRadioButton" || subrole == Some("AXTabButton")
}

fn press(el: &AxElement, window: Option<&AxElement>) -> Result<()> {
    info!(element = %ax_summary(el), "AX press: begin");
    let err = el.perform("AXPress");
    if err == K_AX_ERROR_SUCCESS {
        info!("AX press: AXPress succeeded");
        return Ok(());
    }
    info!(err, "AX press: AXPress failed");
    if matches!(
        err,
        K_AX_ERROR_ACTION_UNSUPPORTED | K_AX_ERROR_API_DISABLED | K_AX_ERROR_ACTION_UNSUPPORTED_ALT
    ) {
        if let Some(window) = window {
            info!("AX press: fallback activate and raise window");
            match activate_xcode() {
                Ok(()) => info!("AX press: activated Xcode"),
                Err(error) => warn!(%error, "AX press: activate Xcode failed"),
            }
            thread::sleep(Duration::from_millis(150));
            let raise_err = window.perform("AXRaise");
            info!(raise_err, "AX press: raised window");
            thread::sleep(Duration::from_millis(150));
        }
        if click_element(el) {
            info!("AX press: click fallback succeeded");
            return Ok(());
        }
        warn!("AX press: click fallback failed");
    }
    ax_ok(err, "AXPress")
}

fn click_element(el: &AxElement) -> bool {
    info!(element = %ax_summary(el), "AX click element: begin");
    let Some(pos) = el.point_attr("AXPosition") else {
        warn!("AX click element: missing AXPosition");
        return false;
    };
    let Some(size) = el.size_attr("AXSize") else {
        warn!("AX click element: missing AXSize");
        return false;
    };
    if size.width <= 0.0 || size.height <= 0.0 {
        warn!(
            width = size.width,
            height = size.height,
            "AX click element: invalid size"
        );
        return false;
    }
    let point = CGPoint {
        x: pos.x + size.width / 2.0,
        y: pos.y + size.height / 2.0,
    };
    info!(
        x = point.x,
        y = point.y,
        width = size.width,
        height = size.height,
        "AX click element: posting mouse events"
    );
    unsafe {
        let down = CGEventCreateMouseEvent(
            std::ptr::null(),
            K_CG_EVENT_LEFT_MOUSE_DOWN,
            point,
            0 as c_uint,
        );
        let up = CGEventCreateMouseEvent(
            std::ptr::null(),
            K_CG_EVENT_LEFT_MOUSE_UP,
            point,
            0 as c_uint,
        );
        if down.is_null() || up.is_null() {
            warn!(
                down_null = down.is_null(),
                up_null = up.is_null(),
                "AX click element: failed to create mouse events"
            );
            if !down.is_null() {
                CFRelease(down);
            }
            if !up.is_null() {
                CFRelease(up);
            }
            return false;
        }
        CGEventPost(K_CG_HID_EVENT_TAP, down);
        thread::sleep(Duration::from_millis(40));
        CGEventPost(K_CG_HID_EVENT_TAP, up);
        CFRelease(down);
        CFRelease(up);
    }
    info!("AX click element: success");
    true
}

fn post_key(key_code: u16, flags: u64) -> Result<()> {
    info!(key_code, flags, "AX post key: begin");
    unsafe {
        let down = CGEventCreateKeyboardEvent(std::ptr::null(), key_code, 1);
        let up = CGEventCreateKeyboardEvent(std::ptr::null(), key_code, 0);
        if down.is_null() || up.is_null() {
            warn!(
                key_code,
                flags,
                down_null = down.is_null(),
                up_null = up.is_null(),
                "AX post key: failed to create keyboard events"
            );
            if !down.is_null() {
                CFRelease(down);
            }
            if !up.is_null() {
                CFRelease(up);
            }
            return Err(Error::InvalidInput(
                "failed to create keyboard event".to_owned(),
            ));
        }
        CGEventSetFlags(down, flags);
        CGEventSetFlags(up, flags);
        CGEventPost(K_CG_HID_EVENT_TAP, down);
        thread::sleep(Duration::from_millis(40));
        CGEventPost(K_CG_HID_EVENT_TAP, up);
        CFRelease(down);
        CFRelease(up);
    }
    info!(key_code, flags, "AX post key: success");
    Ok(())
}

fn ax_ok(err: AXError, operation: &str) -> Result<()> {
    if err == K_AX_ERROR_SUCCESS {
        Ok(())
    } else {
        Err(Error::InvalidInput(format!(
            "{operation} failed: AX error {err}"
        )))
    }
}

fn cf_string(value: &str) -> Option<CfOwned> {
    let c_value = CString::new(value).ok()?;
    let ptr = unsafe {
        CFStringCreateWithCString(
            std::ptr::null(),
            c_value.as_ptr(),
            K_CF_STRING_ENCODING_UTF8,
        )
    };
    CfOwned::new(ptr)
}

fn cf_to_string(value: CFTypeRef) -> Option<String> {
    if value.is_null() || !cf_is_type(value, unsafe { CFStringGetTypeID() }) {
        return None;
    }
    let length = unsafe { CFStringGetLength(value) };
    let size = length.saturating_mul(4).saturating_add(1);
    let mut buffer = vec![0u8; size as usize];
    let ok = unsafe {
        CFStringGetCString(
            value,
            buffer.as_mut_ptr().cast::<c_char>(),
            size,
            K_CF_STRING_ENCODING_UTF8,
        )
    };
    if ok == 0 {
        return None;
    }
    CStr::from_bytes_until_nul(&buffer)
        .ok()
        .and_then(|s| s.to_str().ok())
        .map(str::to_owned)
}

fn cf_to_bool(value: CFTypeRef) -> Option<bool> {
    if value.is_null() {
        return None;
    }
    if cf_is_type(value, unsafe { CFBooleanGetTypeID() }) {
        return Some(unsafe { CFBooleanGetValue(value) } != 0);
    }
    if cf_is_type(value, unsafe { CFNumberGetTypeID() }) {
        let mut out: c_int = 0;
        let ok = unsafe {
            CFNumberGetValue(
                value,
                K_CF_NUMBER_SINT32_TYPE,
                (&mut out as *mut c_int).cast(),
            )
        };
        return (ok != 0).then_some(out != 0);
    }
    cf_to_string(value).map(|s| matches!(s.as_str(), "1" | "true" | "yes"))
}

fn cf_is_type(value: CFTypeRef, type_id: CFTypeID) -> bool {
    !value.is_null() && unsafe { CFGetTypeID(value) == type_id }
}

fn optional(value: String) -> Option<String> {
    (!value.is_empty()).then_some(value)
}

fn normalize(value: &str) -> String {
    value.trim().to_ascii_lowercase()
}

fn normalize_menu(value: &str) -> String {
    normalize(value).replace('…', "...")
}
