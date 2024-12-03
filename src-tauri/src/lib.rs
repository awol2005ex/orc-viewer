// Learn more about Tauri commands at https://tauri.app/develop/calling-rust/
mod api;
use api::orc_reader::read_orc_file;
#[cfg_attr(mobile, tauri::mobile_entry_point)]
pub fn run() {
    tauri::Builder::default()
        .plugin(tauri_plugin_dialog::init())
        .plugin(tauri_plugin_shell::init())
        .invoke_handler(tauri::generate_handler![read_orc_file])
        .run(tauri::generate_context!())
        .expect("error while running tauri application");
}
