# DriverTranslator Function Cheat-Sheet

Purpose: fast re-orientation for `main.py` without re-reading ~5k lines.

## Big Picture Flow

1. `main()` parses CLI, configures logging, loads config, validates it, then starts async server.
2. `run_server()` builds runtime dependencies (AMX client, state, notifier, HTTP status server, RTI TCP server).
3. `handle_client()` processes RTI/WyreStorm text commands, mutates emulated matrix state, and forwards relevant actions to AMX.
4. `_handle_http_client()` serves status/controls web UI and JSON control endpoints.

## Core Data Models

- `Tx`: transmitter identity (`alias`, `hostname`, `ip`, `amx_stream`).
- `Rx`: receiver identity (`alias`, `hostname`, `ip`, `amx_decoder_ip`).
- `NhdCtlIdentity`: emulated controller version/network identity values.
- `Config`: flattened runtime config object loaded from JSON.
- `ControllerState`: mutable matrix + device-health state used across sessions.
- `RuntimeSettings`: runtime-toggle values that can be changed from HTTP controls.
- `HealthState`: lightweight health counters (`rti_clients`).
- `ProblemState`: ring-buffer of recent problem messages shown in UI/API.
- `NhdCtlSession`: per-RTI-connection session flags (currently alias mode).

## Config + Persistence Helpers

- `load_config(path)`: parses `config.json`, builds dataclasses and endpoint maps.
- `_validate_config(cfg)`: hard validation for endpoint and port sanity + incompatible modes.
- `_persist_runtime_setting_to_config(...)`: writes runtime toggle changes back into config file.
- `_generate_endpoints_from_size(...)`: creates TX/RX endpoint lists from counts + starting IPs.
- `_persist_endpoints_to_config(...)`: stores generated endpoint inventory into config JSON.
- `_load_endpoint_inventory(...)`: reads endpoint alias/skip flags for UI table.
- `_persist_endpoint_skip_to_config(...)`: updates one endpoint `skip` flag in config.

## Unknown-Command Tracking

- `_unknown_ctl_configure(...)`: chooses storage path for unknown RTI command log.
- `_unknown_ctl_load_from_disk()`: restores persisted unknown-command counters at startup.
- `_unknown_ctl_save_to_disk()`: persists unknown-command counters atomically.
- `_unknown_ctl_clear_persisted()`: clears in-memory and persisted unknown-command history.
- `_unknown_ctl_record(line)`: deduplicates and increments unknown command counts.
- `_unknown_ctl_page_text()`: renders human-readable triage block for status page.

## General Utility Helpers

- `_ctl_json(v)`: formats compact, stable JSON-like output matching NHD-CTL style.
- `_http_parse_path_params(path)`: tiny query-string parser.
- `_http_ui_sess_issue()` / `_http_ui_sess_valid(tok)`: short-lived UI session token issue/validate.
- `_crlf(line)`: RTI line encoder with CRLF terminator.
- `_as_int(...)`, `_as_bool(...)`, `_clamp_int(...)`, `_bind_addr(...)`, `_opt_str(...)`: defensive coercion helpers.
- `_retry_delay_seconds(...)`: exponential retry backoff with jitter.
- `_rx_alias_sort_key(...)`, `_tx_alias_sort_key(...)`: natural sort keys for `OUT#`/`IN#` aliases.

## HTTP Status + Control Surface

- `_http_response(...)`: builds raw HTTP/1.1 response bytes.
- `_http_unauthorized()`: emits 401 with basic-auth challenge header.
- `_parse_basic_auth_password(data)`: extracts password from Basic auth header.
- `_params_want_html(params)`: toggle HTML response mode for control endpoints.
- `_control_feedback_html(...)`: renders styled success/error HTML response pages.
- `_format_uptime(seconds)`: uptime display helper.
- `_build_status_snapshot(...)`: core status payload used by `/status`.
- `_get_log_tail(n)`: reads latest in-memory logs captured by ring handler.
- `_handle_http_client(...)`: full HTTP router (status, logs, controls, self-test, restart/reboot, endpoint sizing, unknown command view).

## RTI Status/Notify + Control

- `RtiNotifier`: sends event/problem messages to RTI over UDP or TCP with anti-spam controls.
  - `start()`: initializes UDP transport if needed.
  - `send(message)`: sends a single notification message.
  - `problem(key, message)`: throttled problems-only send path and local logging.
- `StatusReporter`: periodic status beacons (`DTSTATUS`) via `RtiNotifier`.
  - `start()` / `_loop()`: background timer task.
  - `_send_status()`: emits mode/client/connection summary.
- `_RtiControlUdp`: UDP listener for remote reboot trigger command with cooldown.

## AMX Protocol Helpers

- `_parse_amx_status(data)`: parse AMX `key:value` status packet into dict.
- `_log_amx_inbound(...)`: conditional expanded logging of AMX responses.
- `_hdmi_enabled_from_status_fields(fields)`: normalizes `HDMIOFF` to `True/False/None`.
- `_read_amx_status_fields_from_ip(...)`: one-shot connect + `?` query helper for TX polling.

## AMX Client Implementations

- `AmxClient`: connect-per-command client (stateless), with per-decoder locks and retry for mutating commands.
  - Public API: `set_stream`, `set_hdmi_output`, `get_hdmi_output`, `verify_stream`, `send_command`, plus command+status variants.
  - Internal methods: `_send_locked`, `_send_and_read_status_locked`, and retry wrappers.
- `DryRunAmxClient`: no network I/O; simulates decoder online/offline, stream, and HDMI output.
- `PersistentAmxClient`: one long-lived socket per decoder using `_DecoderWorker`.
  - `_get_worker()`: lazy worker creation.
  - `connection_summary()`: reports connected/known worker count.
- `_DecoderWorker`: persistent per-decoder command loop.
  - Manages reconnects, keepalive, latest-wins queue for `set:` commands, and serialized socket operations.
  - Methods used by outer client mirror AMX operation set (`send_set`, `verify_stream`, `send_command_with_status`, etc.).

## Matrix/Device Emulation

- `_lookup_tx(...)` / `_lookup_rx(...)`: resolve alias or hostname.
- `_tx_alias_from_amx_stream(...)`: map AMX STREAM value back to TX alias.
- `_all_endpoint_aliases(...)`: list all configured aliases.
- `_emulated_multicast_ips(...)`: deterministic fake multicast values for status payloads.
- `_device_status_tx_dict(tx)`: emulated TX status row.
- `_device_status_rx_dict(rx, state)`: emulated RX status row reflecting online/routing/HDMI state.
- `_format_tx_signal(fields)`: converts TX status fields to human UI signal string/class.
- `_format_matrix_info(...)`: formats matrix get response block.
- `_as_success(line)`: appends `success` suffix when protocol expects it.
- `_handle_multiview_get(...)`: minimal mscene/mview query emulation.
- `_handle_videowall_get(...)`: minimal scene/vw/wscene2 query emulation.
- `_handle_config_get(...)`: large config query response table for RTI driver compatibility.

## Routing + Status Refresh Workflows

- `_handle_matrix_set(...)`: parses matrix-set command, applies AMX command fanout, returns mirror ack + failures.
- `_apply_amx_command_to_rx_aliases(...)`: fanout helper for AMX commands over RX set, gathering failures/status.
- `_refresh_hdmi_outputs(...)`: poll HDMI on all RX and update `ControllerState`.
- `_refresh_hdmi_outputs_for_aliases(...)`: scoped HDMI poll for touched RX list.
- `_refresh_tx_statuses(...)`: poll TX status fields for all TX endpoints.
- `TxStatusPoller`: background loop to refresh TX status every `_TX_STATUS_POLL_INTERVAL_SECONDS`.

## RTI TCP Command Handler

- `handle_client(...)`: main RTI line protocol loop.
  - Handles `matrix set`, breakaway (`matrix video|audio|... set`), matrix queries, config gets/sets, CEC-to-AMX translation, scene/multiview/videowall surfaces, and unknown-command recording.
  - Uses command mirror acknowledgements to keep RTI/WyreStorm driver behavior stable.

## Startup/Runtime Lifecycle

- `_open_connection(...)`: async TCP helper with optional bind address.
- `_amx_self_test(...)`: startup decoder reachability test.
- `_do_reboot(...)`: OS reboot command trigger.
- `_do_service_restart(...)`: `systemctl restart drivertranslator`.
- `run_server(...)`: wires all subsystems, starts listeners/tasks, and serves forever.
- `main(argv)`: CLI entrypoint.

## Fast “Where To Look” Pointers

- RTI command behavior: `handle_client()` and `_handle_config_get()`.
- HTTP UI/API controls: `_handle_http_client()`.
- AMX integration details: `AmxClient`, `PersistentAmxClient`, `_DecoderWorker`.
- Routing outcomes/state drift: `_apply_amx_command_to_rx_aliases()`, `ControllerState`.
- Unknown command triage: `_unknown_ctl_*` helpers + HTTP unknown command block.
