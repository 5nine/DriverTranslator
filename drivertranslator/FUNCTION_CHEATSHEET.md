# DriverTranslator function cheat-sheet

Purpose: quick map of **where code lives** after the `main.py` split. For behavior details, open the listed module.

## Module map (high level)

| Area | Primary modules |
|------|-----------------|
| CLI entry | `main.py` → `asyncio.run(run_server(...))` |
| Listener bootstrap | `server.py` (`run_server`) |
| RTI TCP session | `rti_tcp.py` (`handle_client`) |
| NHD `config get` + multiview/videowall stubs | `nhd_ctl_handlers.py` |
| Matrix routes, AMX fanout, TX polling | `matrix_amx.py` |
| HTTP status / control UI | `http_status.py` (`handle_http_client`) |
| HTTP primitives | `http_helpers.py` |
| Shared constants | `constants.py` (e.g. `TX_STATUS_POLL_INTERVAL_SECONDS`) |
| Decoder reachability self-test | `amx_self_test.py` (`amx_self_test`) |
| UDP reboot listener | `rti_control_udp.py` (`RtiControlUdp`) |
| Config load/validate | `config_loader.py` |
| JSON config edits (UI / persistence) | `config_persistence.py` |
| Unknown-command tracking | `unknown_ctl.py` |
| AMX TCP clients | `amx_client.py` |
| AMX line protocol | `amx_protocol.py` |
| Lookups + matrix formatting | `protocol_helpers.py` |
| Problem notifications (throttled) | `problem_reporter.py` (`LocalProblemReporter`) |
| Models / state | `models.py` |

Run as a module: `python3 -m drivertranslator` (see `__main__.py`).

## Big-picture flow

1. **`main()`** (`main.py`): argparse, logging + ring handler, `load_config` / `validate_config`, then `asyncio.run(run_server(...))`.
2. **`run_server()`** (`server.py`): unknown-ctl setup, AMX client selection, `ControllerState` / notifier / runtime, optional HTTP server, RTI TCP server, optional startup self-test task.
3. **`handle_client()`** (`rti_tcp.py`): per-connection RTI line loop (matrix, breakaway, config get/set, CEC→AMX, multiview/videowall stubs, unknown-command recording).
4. **`handle_http_client()`** (`http_status.py`): HTTP status JSON, logs, control API, HTML UI.

## Core data models (`models.py`)

- **`Tx`**, **`Rx`**: endpoint identity and AMX/stream fields.
- **`NhdCtlIdentity`**: emulated controller version/network identity.
- **`Config`**: loaded from JSON.
- **`ControllerState`**: matrix + device health state.
- **`RuntimeSettings`**: toggles (incl. HTTP-updated values mirrored from config).
- **`HealthState`**: e.g. `rti_clients` count.
- **`ProblemState`**: recent problems for UI/API.
- **`NhdCtlSession`**: per-RTI-connection flags (e.g. alias mode).

## Config + persistence

- **`load_config`**, **`validate_config`**: `config_loader.py`.
- **`ctl_json`**, **`generate_endpoints_from_size`**, **`load_endpoint_inventory`**, **`persist_endpoints_to_config`**, **`persist_endpoint_skip_to_config`**, **`persist_runtime_setting_to_config`**: `config_persistence.py`.

## Unknown-command tracking (`unknown_ctl.py`)

- **`configure`**, **`load_from_disk`**, **`record`**, **`page_text`**, **`persist_file`**, **`clear_persisted`**, etc.

## Utilities (`utils.py`)

- **`as_int`**, **`as_bool`**, **`clamp_int`**, **`bind_addr`**, **`opt_str`**, **`retry_delay_seconds`**, **`rx_alias_sort_key`**, **`tx_alias_sort_key`**, etc.

## HTTP (`http_helpers.py`, `http_ui_session.py`, `http_status.py`)

- **`http_response`**, **`http_unauthorized`**, **`parse_basic_auth_password`**, **`params_want_html`**, **`control_feedback_html`**, **`format_uptime`**, **`build_status_snapshot`**: `http_helpers.py`.
- **`parse_path_params`**, **`issue_session_token`**, **`valid_session_token`**: `http_ui_session.py`.
- **`get_log_tail`**: `log_ring.py`.
- **`handle_http_client`**: full router in `http_status.py`.

## Problems (`problem_reporter.py`)

- **`LocalProblemReporter`**: throttled `problem(...)` used from `rti_tcp` / startup (not the old `RtiNotifier` / `StatusReporter` names).

## AMX protocol (`amx_protocol.py`)

- **`parse_amx_status`**, **`log_amx_inbound`**, **`hdmi_enabled_from_status_fields`**.

## AMX clients (`amx_client.py`)

- **`AmxClient`**, **`DryRunAmxClient`**, **`PersistentAmxClient`**, **`DecoderWorker`** (persistent per-decoder loop).

## Matrix / routing (`protocol_helpers.py` + `matrix_amx.py`)

- Lookups / matrix text: **`lookup_tx`**, **`lookup_rx`**, **`tx_alias_from_amx_stream`**, **`all_endpoint_aliases`**, **`device_status_tx_dict`**, **`device_status_rx_dict`**, **`format_matrix_info`**, **`format_tx_signal`**, **`as_success`**: `protocol_helpers.py`.
- Routing execution: **`apply_amx_command_to_rx_aliases`**, **`handle_matrix_set`**, **`read_amx_status_fields_from_ip`**, **`refresh_tx_statuses`**, **`refresh_rx_statuses`**, **`TxStatusPoller`**, **`RxStatusPoller`**: `matrix_amx.py`.
- TX poll interval: **`TX_STATUS_POLL_INTERVAL_SECONDS`** in `constants.py` (used by `matrix_amx` and `http_status` HTML copy).

## NHD query stubs (`nhd_ctl_handlers.py`)

- **`handle_config_get`**, **`handle_multiview_get`**, **`handle_videowall_get`**.

## Networking (`networking.py`)

- **`open_connection`**, **`crlf_line`**.

## OS / service control (`system_control.py`)

- **`do_reboot`**, **`do_service_restart`**.

## Startup / diagnostics

- **`amx_self_test`**: `amx_self_test.py` (also passed into HTTP self-test).
- **`RtiControlUdp`**: `rti_control_udp.py`.

## Fast “where to look”

| Task | Module / symbol |
|------|-----------------|
| RTI command behavior | `rti_tcp.handle_client`, `nhd_ctl_handlers.handle_config_get` |
| HTTP UI / `/control/*` | `http_status.handle_http_client` |
| AMX send / verify | `amx_client` classes, `matrix_amx.apply_amx_command_to_rx_aliases` |
| State updates | `models.ControllerState` |
| Unknown commands | `unknown_ctl` + status page section in `http_status` |
| Add a new constant shared by HTTP + matrix | `constants.py` |
