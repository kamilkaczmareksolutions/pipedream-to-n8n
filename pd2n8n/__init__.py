from __future__ import annotations

import json
import re
import uuid
import zipfile
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from typing import Any, Never, NoReturn, TypedDict


class CodeLanguage(Enum):
    JAVASCRIPT = "javascript"
    PYTHON = "python"
    OTHER = "other"


class HttpMethod(Enum):
    GET = "GET"
    POST = "POST"
    PUT = "PUT"
    PATCH = "PATCH"
    DELETE = "DELETE"
    HEAD = "HEAD"


class SkippedKind(Enum):
    TRIGGER = "trigger"
    STEP = "step"


class SkippedExportStatus(Enum):
    NEVER_DEPLOYED = "skipped_never_deployed"
    EXPORT_LIMIT = "skipped_export_limit"


@dataclass(frozen=True)
class FiveFieldCron:
    expression: str


@dataclass(frozen=True)
class _NodeName:
    value: str


@dataclass(frozen=True)
class CronTrigger:
    cron: FiveFieldCron
    timezone: str | None
    ignored_interval_seconds: int | None = None


@dataclass(frozen=True)
class IntervalTrigger:
    interval_seconds: int
    timezone: str | None


@dataclass(frozen=True)
class HttpTrigger:
    method: HttpMethod | None
    path: str | None
    prop_names: tuple[str, ...]
    redacted_names: tuple[str, ...]
    auth_provision_id: str | None
    redacted: bool


@dataclass(frozen=True)
class EmailTrigger:
    auth_provision_id: str | None
    redacted: bool
    prop_names: tuple[str, ...]


@dataclass(frozen=True)
class FormTrigger:
    auth_provision_id: str | None
    redacted: bool
    prop_names: tuple[str, ...]


@dataclass(frozen=True)
class SourceTrigger:
    component_key: str | None
    auth_provision_id: str | None
    redacted: bool
    prop_names: tuple[str, ...]


@dataclass(frozen=True)
class UnrecognizedTrigger:
    wire_type: str
    auth_provision_id: str | None
    redacted: bool
    prop_names: tuple[str, ...]


Trigger = (
    CronTrigger
    | IntervalTrigger
    | HttpTrigger
    | EmailTrigger
    | FormTrigger
    | SourceTrigger
    | UnrecognizedTrigger
)


@dataclass(frozen=True)
class CodeStep:
    namespace: str
    language: CodeLanguage
    runtime: str
    source: str


@dataclass(frozen=True)
class ActionStep:
    namespace: str
    key: str
    version: str | None
    auth_provision_id: str | None
    redacted: bool
    prop_names: tuple[str, ...]
    configured: tuple[tuple[str, str], ...]


@dataclass(frozen=True)
class UnmappedBranch:
    namespace: str
    nested_namespaces: tuple[str, ...]


@dataclass(frozen=True)
class UnrecognizedStep:
    namespace: str
    auth_provision_id: str | None
    redacted: bool
    prop_names: tuple[str, ...]


Step = CodeStep | ActionStep | UnmappedBranch | UnrecognizedStep


@dataclass(frozen=True)
class Skipped:
    kind: SkippedKind
    name: str
    nested: tuple[str, ...]


@dataclass(frozen=True)
class Workflow:
    name: str
    description: str
    inactive: bool
    triggers: tuple[Trigger, ...]
    steps: tuple[Step, ...]
    skipped: tuple[Skipped, ...]


@dataclass(frozen=True)
class LoadedWorkflow:
    output_stem: str
    workflow: Workflow


@dataclass(frozen=True)
class SkippedExport:
    name: str
    status: SkippedExportStatus
    warnings: tuple[str, ...]


@dataclass(frozen=True)
class LoadedExport:
    workflows: tuple[LoadedWorkflow, ...]
    skipped_exports: tuple[SkippedExport, ...]


@dataclass(frozen=True)
class ManualItem:
    node_name: str
    headline: str
    component_key: str | None
    version: str | None
    auth_provision_id: str | None
    redacted: bool
    names: tuple[str, ...]


@dataclass(frozen=True)
class Report:
    mapped: tuple[str, ...]
    manual: tuple[ManualItem, ...]
    notes: tuple[str, ...]


class N8nNode(TypedDict):
    parameters: dict[str, Any]
    id: str
    name: str
    type: str
    typeVersion: float
    position: list[int]


class N8nConnection(TypedDict):
    node: str
    type: str
    index: int


class N8nDocument(TypedDict):
    name: str
    nodes: list[N8nNode]
    connections: dict[str, dict[str, list[list[N8nConnection]]]]
    settings: dict[str, str]
    pinData: dict[str, Any]


@dataclass(frozen=True)
class Conversion:
    document: N8nDocument
    report: Report


class LoadError(Exception):
    pass


__all__ = (
    "ActionStep",
    "CodeLanguage",
    "CodeStep",
    "Conversion",
    "CronTrigger",
    "EmailTrigger",
    "FiveFieldCron",
    "FormTrigger",
    "HttpMethod",
    "HttpTrigger",
    "IntervalTrigger",
    "LoadError",
    "LoadedExport",
    "LoadedWorkflow",
    "ManualItem",
    "N8nConnection",
    "N8nDocument",
    "N8nNode",
    "Report",
    "Skipped",
    "SkippedExport",
    "SkippedExportStatus",
    "SkippedKind",
    "SourceTrigger",
    "Step",
    "Trigger",
    "UnmappedBranch",
    "UnrecognizedStep",
    "UnrecognizedTrigger",
    "Workflow",
    "format_report",
    "load",
    "walk",
)

_STEP_RETURN_RE = re.compile(
    r"steps\.([A-Za-z_][A-Za-z0-9_]*)\.\$return_value"
)
_HTTP_KNOWN_PROPS = frozenset({"method", "httpMethod", "path"})


def load(path: Path) -> LoadedExport:
    if not path.exists():
        raise LoadError(f"Path not found: {path}")
    suffix = path.suffix.lower()
    if suffix == ".zip":
        return _load_zip(path)
    if suffix == ".json":
        return _load_single_json(path)
    raise LoadError(f"Unsupported file type: {path}")


def _load_single_json(path: Path) -> LoadedExport:
    raw = _read_json_file(path)
    if _is_project_manifest(raw):
        raise LoadError("JSON file is a project manifest, not a workflow")
    if not _looks_like_workflow(raw):
        raise LoadError("JSON file is not a Pipedream workflow export")
    stem = path.stem
    return LoadedExport(
        workflows=(_parse_workflow(raw, stem),),
        skipped_exports=(),
    )


def _read_json_file(path: Path) -> Mapping[str, Any]:
    try:
        text = path.read_text(encoding="utf-8")
        data = json.loads(text)
    except (OSError, json.JSONDecodeError) as exc:
        raise LoadError(f"Invalid JSON: {path}") from exc
    if not isinstance(data, dict):
        raise LoadError(f"Invalid JSON: {path}")
    return data


def _is_project_manifest(raw: Mapping[str, Any]) -> bool:
    schema = raw.get("schema")
    return isinstance(schema, str) and schema.startswith("project-export/")


def _looks_like_workflow(raw: Mapping[str, Any]) -> bool:
    return isinstance(raw.get("name"), str) and (
        "triggers" in raw or "steps" in raw
    )


def _load_zip(path: Path) -> LoadedExport:
    try:
        zf = zipfile.ZipFile(path, "r")
    except zipfile.BadZipFile as exc:
        raise LoadError(f"Invalid zip: {path}") from exc
    with zf:
        try:
            manifest_bytes = zf.read("manifest.json")
        except KeyError as exc:
            raise LoadError("Zip has no manifest.json") from exc
        try:
            manifest = json.loads(manifest_bytes.decode("utf-8"))
        except json.JSONDecodeError as exc:
            raise LoadError("Invalid manifest.json") from exc
        if not isinstance(manifest, dict):
            raise LoadError("Invalid manifest.json")
        schema = manifest.get("schema")
        if schema != "project-export/2026.01":
            raise LoadError(f"Unsupported manifest schema: {schema!r}")
        workflows_raw = manifest.get("workflows")
        if not isinstance(workflows_raw, list):
            raise LoadError("Invalid manifest workflows list")

        loaded: list[LoadedWorkflow] = []
        skipped_exports: list[SkippedExport] = []
        stem_counts: dict[str, int] = {}

        for entry in workflows_raw:
            if not isinstance(entry, dict):
                raise LoadError("Invalid manifest workflow row")
            status = entry.get("status")
            name = entry.get("name")
            if not isinstance(name, str):
                raise LoadError("Manifest workflow row missing name")
            if status == "exported":
                entry_path = entry.get("path")
                if not isinstance(entry_path, str):
                    raise LoadError(f"Exported workflow {name!r} missing path")
                try:
                    wf_bytes = zf.read(entry_path)
                except KeyError as exc:
                    raise LoadError(
                        f"Workflow file missing in zip: {entry_path}"
                    ) from exc
                try:
                    wf_raw = json.loads(wf_bytes.decode("utf-8"))
                except json.JSONDecodeError as exc:
                    raise LoadError(f"Invalid workflow JSON: {entry_path}") from exc
                if not isinstance(wf_raw, dict):
                    raise LoadError(f"Invalid workflow JSON: {entry_path}")
                base_stem = Path(entry_path).stem
                stem_counts[base_stem] = stem_counts.get(base_stem, 0) + 1
                count = stem_counts[base_stem]
                output_stem = base_stem if count == 1 else f"{base_stem}-{count}"
                loaded.append(_parse_workflow(wf_raw, output_stem))
            elif status == "skipped_never_deployed":
                skipped_exports.append(
                    SkippedExport(
                        name=name,
                        status=SkippedExportStatus.NEVER_DEPLOYED,
                        warnings=_warnings_tuple(entry),
                    )
                )
            elif status == "skipped_export_limit":
                skipped_exports.append(
                    SkippedExport(
                        name=name,
                        status=SkippedExportStatus.EXPORT_LIMIT,
                        warnings=_warnings_tuple(entry),
                    )
                )
            else:
                raise LoadError(f"Unsupported manifest workflow status: {status!r}")

        if not loaded:
            raise LoadError("Zip contains no exported workflow JSON")
        return LoadedExport(
            workflows=tuple(loaded),
            skipped_exports=tuple(skipped_exports),
        )


def _warnings_tuple(entry: Mapping[str, Any]) -> tuple[str, ...]:
    warnings = entry.get("warnings")
    if not isinstance(warnings, list):
        return ()
    return tuple(str(w) for w in warnings)


def _parse_workflow(raw: Mapping[str, Any], output_stem: str) -> LoadedWorkflow:
    name = raw.get("name")
    if not isinstance(name, str):
        raise LoadError("Workflow missing name")
    description = raw.get("description")
    if not isinstance(description, str):
        description = ""
    inactive = bool(raw.get("inactive", False))

    triggers_raw = raw.get("triggers")
    if not isinstance(triggers_raw, list):
        triggers_raw = []
    steps_raw = raw.get("steps")
    if not isinstance(steps_raw, list):
        steps_raw = []
    deployments_raw = raw.get("deployments")
    if deployments_raw is None:
        deployments_raw = {}
    if not isinstance(deployments_raw, dict):
        raise LoadError("Invalid deployments object")

    active_step_names: set[str] = set()
    skipped: list[Skipped] = []
    steps: list[Step] = []

    for step_raw in steps_raw:
        if not isinstance(step_raw, dict):
            raise LoadError("Invalid step entry")
        namespace = step_raw.get("namespace")
        if not isinstance(namespace, str):
            raise LoadError("Step missing namespace")
        if step_raw.get("disabled"):
            skipped.append(
                Skipped(kind=SkippedKind.STEP, name=namespace, nested=())
            )
            continue
        active_step_names.add(namespace)
        steps.append(_parse_step(step_raw, namespace))

    triggers: list[Trigger] = []
    for trig_raw in triggers_raw:
        if not isinstance(trig_raw, dict):
            raise LoadError("Invalid trigger entry")
        trig_type = trig_raw.get("type")
        if not isinstance(trig_type, str):
            raise LoadError("Trigger missing type")
        if trig_raw.get("disabled"):
            skipped.append(
                Skipped(kind=SkippedKind.TRIGGER, name=trig_type, nested=())
            )
            continue
        triggers.append(_parse_trigger(trig_raw, trig_type))

    for dep_id, dep_body in deployments_raw.items():
        if not isinstance(dep_id, str):
            continue
        nested = _deployment_nested_namespaces(dep_body)
        unmapped = tuple(ns for ns in nested if ns not in active_step_names)
        steps.append(
            UnmappedBranch(
                namespace=dep_id,
                nested_namespaces=unmapped if unmapped else nested,
            )
        )

    workflow = Workflow(
        name=name,
        description=description,
        inactive=inactive,
        triggers=tuple(triggers),
        steps=tuple(steps),
        skipped=tuple(skipped),
    )
    return LoadedWorkflow(output_stem=output_stem, workflow=workflow)


def _deployment_nested_namespaces(dep_body: Any) -> tuple[str, ...]:
    if not isinstance(dep_body, dict):
        return ()
    steps = dep_body.get("steps")
    if not isinstance(steps, list):
        return ()
    names: list[str] = []
    for item in steps:
        if isinstance(item, dict):
            ns = item.get("namespace")
            if isinstance(ns, str):
                names.append(ns)
    return tuple(names)


def _configured_props(props: Mapping[str, Any]) -> tuple[tuple[str, str], ...]:
    items: list[tuple[str, str]] = []
    for key, value in props.items():
        if isinstance(value, (str, int, float, bool)) and value != "<redacted>":
            items.append((key, str(value)))
    return tuple(items)


def _sensitive_prop_names(props: Mapping[str, Any]) -> tuple[str, ...]:
    names: list[str] = []
    for key, value in props.items():
        if value == "<redacted>":
            names.append(key)
        elif isinstance(value, dict) and "authProvisionId" in value:
            names.append(key)
    return tuple(names)


def _http_unrecognized_props(props: Mapping[str, Any]) -> tuple[str, ...]:
    names: list[str] = []
    for key, value in props.items():
        if key in _HTTP_KNOWN_PROPS:
            continue
        if value == "<redacted>":
            continue
        if isinstance(value, dict) and "authProvisionId" in value:
            continue
        names.append(key)
    return tuple(names)


def _lift_props(props: Mapping[str, Any]) -> tuple[str | None, bool, tuple[str, ...]]:
    auth_id: str | None = None
    redacted = False
    unknown: list[str] = []
    for key, value in props.items():
        if value == "<redacted>":
            redacted = True
            continue
        if isinstance(value, dict) and "authProvisionId" in value:
            apn = value.get("authProvisionId")
            if isinstance(apn, str):
                auth_id = apn
            continue
        if isinstance(value, dict):
            unknown.append(key)
            continue
        unknown.append(key)
    return auth_id, redacted, tuple(unknown)


def _parse_trigger(trig_raw: Mapping[str, Any], trig_type: str) -> Trigger:
    props_raw = trig_raw.get("props")
    props: Mapping[str, Any] = props_raw if isinstance(props_raw, dict) else {}
    auth_id, redacted, unknown = _lift_props(props)

    if trig_type == "timer":
        cron_val = props.get("cron")
        interval_val = props.get("interval_seconds")
        timezone = props.get("timezone")
        tz = timezone if isinstance(timezone, str) else None
        has_cron = isinstance(cron_val, str) and cron_val.strip()
        has_interval = isinstance(interval_val, int) and interval_val > 0
        if has_cron and has_interval:
            _validate_five_field_cron(cron_val)
            return CronTrigger(
                cron=FiveFieldCron(cron_val.strip()),
                timezone=tz,
                ignored_interval_seconds=int(interval_val),
            )
        if has_cron:
            _validate_five_field_cron(cron_val)
            return CronTrigger(cron=FiveFieldCron(cron_val.strip()), timezone=tz)
        if has_interval:
            return IntervalTrigger(interval_seconds=int(interval_val), timezone=tz)
        return UnrecognizedTrigger(
            wire_type="timer",
            auth_provision_id=auth_id,
            redacted=redacted,
            prop_names=unknown,
        )

    if trig_type == "http":
        method_raw = props.get("method")
        if method_raw is None:
            method_raw = props.get("httpMethod")
        method = _parse_http_method(method_raw)
        path_val = props.get("path")
        path = path_val if isinstance(path_val, str) else None
        return HttpTrigger(
            method=method,
            path=path,
            prop_names=_http_unrecognized_props(props),
            redacted_names=tuple(
                key for key, value in props.items() if value == "<redacted>"
            ),
            auth_provision_id=auth_id,
            redacted=redacted,
        )

    if trig_type == "email":
        return EmailTrigger(
            auth_provision_id=auth_id,
            redacted=redacted,
            prop_names=unknown,
        )
    if trig_type == "form":
        return FormTrigger(
            auth_provision_id=auth_id,
            redacted=redacted,
            prop_names=unknown,
        )
    if trig_type == "source":
        ck = props.get("component_key")
        component_key = ck if isinstance(ck, str) else None
        return SourceTrigger(
            component_key=component_key,
            auth_provision_id=auth_id,
            redacted=redacted,
            prop_names=unknown,
        )
    return UnrecognizedTrigger(
        wire_type=trig_type,
        auth_provision_id=auth_id,
        redacted=redacted,
        prop_names=unknown,
    )


def _validate_five_field_cron(cron_val: str) -> None:
    parts = cron_val.strip().split()
    if len(parts) != 5:
        raise LoadError(f"Cron expression must have five fields: {cron_val!r}")


def _parse_http_method(value: Any) -> HttpMethod | None:
    if not isinstance(value, str):
        return None
    upper = value.upper()
    try:
        return HttpMethod(upper)
    except ValueError:
        return None


def _parse_step(step_raw: Mapping[str, Any], namespace: str) -> Step:
    uses_raw = step_raw.get("uses")
    if not isinstance(uses_raw, dict):
        return UnrecognizedStep(
            namespace=namespace,
            auth_provision_id=None,
            redacted=False,
            prop_names=(),
        )
    props_raw = step_raw.get("props")
    props: Mapping[str, Any] = props_raw if isinstance(props_raw, dict) else {}
    auth_id, redacted, unknown = _lift_props(props)
    lang_raw = step_raw.get("lang")
    runtime = lang_raw if isinstance(lang_raw, str) else ""

    if uses_raw.get("registry") is True:
        key = uses_raw.get("key")
        if not isinstance(key, str):
            key = step_raw.get("component_key")
        if not isinstance(key, str):
            key = namespace
        version_raw = uses_raw.get("version")
        version = version_raw if isinstance(version_raw, str) else None
        return ActionStep(
            namespace=namespace,
            key=key,
            version=version,
            auth_provision_id=auth_id,
            redacted=redacted,
            prop_names=_sensitive_prop_names(props),
            configured=_configured_props(props),
        )

    code = uses_raw.get("code")
    if isinstance(code, str):
        return CodeStep(
            namespace=namespace,
            language=_classify_lang(runtime),
            runtime=runtime,
            source=code,
        )

    return UnrecognizedStep(
        namespace=namespace,
        auth_provision_id=auth_id,
        redacted=redacted,
        prop_names=unknown,
    )


def _classify_lang(runtime: str) -> CodeLanguage:
    lower = runtime.lower()
    if lower.startswith("node") or lower.startswith("javascript"):
        return CodeLanguage.JAVASCRIPT
    if lower.startswith("python"):
        return CodeLanguage.PYTHON
    return CodeLanguage.OTHER


def walk(workflow: Workflow) -> Conversion:
    mapped: list[str] = []
    manual: list[ManualItem] = []
    notes: list[str] = []

    for sk in workflow.skipped:
        notes.append(f"Skipped disabled {sk.kind.value}: {sk.name}")

    step_node_names: dict[str, _NodeName] = {}
    used_names: set[str] = set()

    def allocate_step_name(namespace: str) -> _NodeName:
        base = namespace
        candidate = base
        suffix = 2
        while candidate in used_names:
            candidate = f"{namespace}_{suffix}"
            suffix += 1
            notes.append(
                f"Step namespace {namespace!r} collided; node renamed to {candidate!r}"
            )
        used_names.add(candidate)
        nn = _NodeName(candidate)
        step_node_names[namespace] = nn
        return nn

    trigger_node_names: list[_NodeName] = []
    step_chain: list[_NodeName] = []
    active_triggers = workflow.triggers
    for idx, _trig in enumerate(active_triggers):
        tname = "trigger" if idx == 0 else f"trigger_{idx + 1}"
        used_names.add(tname)
        trigger_node_names.append(_NodeName(tname))

    first_trigger_node = trigger_node_names[0] if trigger_node_names else None

    for step in workflow.steps:
        if isinstance(step, CodeStep):
            allocate_step_name(step.namespace)
        elif isinstance(step, (ActionStep, UnmappedBranch, UnrecognizedStep)):
            allocate_step_name(step.namespace)

    schedule_timezones: list[str | None] = []
    x_pos = 240
    y_pos = 300
    nodes: list[N8nNode] = []
    node_positions: dict[str, int] = {}

    def add_chain_node(
        node_name: _NodeName,
        ntype: str,
        type_version: float,
        parameters: dict[str, Any],
        *,
        chain: bool,
    ) -> None:
        nonlocal x_pos
        nodes.append(
            {
                "parameters": parameters,
                "id": _node_id(workflow.name, node_name),
                "name": node_name.value,
                "type": ntype,
                "typeVersion": type_version,
                "position": [x_pos, y_pos],
            }
        )
        node_positions[node_name.value] = x_pos
        if chain:
            step_chain.append(node_name)
        x_pos += 220

    def add_sticky(for_node: _NodeName, content: str) -> None:
        sticky_name = _NodeName(f"note_{for_node.value}")
        while sticky_name.value in used_names:
            sticky_name = _NodeName(f"{sticky_name.value}_2")
        used_names.add(sticky_name.value)
        anchor_x = node_positions.get(for_node.value, 240)
        nodes.append(
            {
                "parameters": {
                    "content": content,
                    "height": 180,
                    "width": 280,
                },
                "id": _node_id(workflow.name, sticky_name),
                "name": sticky_name.value,
                "type": "n8n-nodes-base.stickyNote",
                "typeVersion": 1,
                "position": [anchor_x, y_pos + 160],
            }
        )

    for idx, trig in enumerate(active_triggers):
        tnode = trigger_node_names[idx]
        match trig:
            case CronTrigger(cron=cron, timezone=tz, ignored_interval_seconds=ignored):
                schedule_timezones.append(tz)
                if ignored is not None:
                    notes.append(
                        f"Timer had both cron and interval_seconds; ignored interval_seconds={ignored}"
                    )
                add_chain_node(
                    tnode,
                    "n8n-nodes-base.scheduleTrigger",
                    1.2,
                    {
                        "rule": {
                            "interval": [
                                {
                                    "field": "cronExpression",
                                    "expression": cron.expression,
                                }
                            ]
                        }
                    },
                    chain=False,
                )
                mapped.append(
                    f"Schedule trigger {tnode.value}: cron {cron.expression!r}"
                )
            case IntervalTrigger(interval_seconds=seconds, timezone=tz):
                schedule_timezones.append(tz)
                interval_param = _interval_parameters(seconds)
                add_chain_node(
                    tnode,
                    "n8n-nodes-base.scheduleTrigger",
                    1.2,
                    {"rule": {"interval": [interval_param]}},
                    chain=False,
                )
                mapped.append(
                    f"Schedule trigger {tnode.value}: every {seconds}s"
                )
            case HttpTrigger(method=method, path=path, prop_names=prop_names, redacted_names=redacted_names, auth_provision_id=auth_id, redacted=redacted):
                http_method = method
                if http_method is None:
                    http_method = HttpMethod.POST
                    notes.append(
                        f"HTTP trigger {tnode.value}: missing method, defaulting to POST"
                    )
                webhook_path = path if path else _slugify(workflow.name)
                add_chain_node(
                    tnode,
                    "n8n-nodes-base.webhook",
                    2,
                    {
                        "httpMethod": http_method.value,
                        "path": webhook_path,
                        "responseMode": "onReceived",
                        "options": {},
                    },
                    chain=False,
                )
                mapped.append(
                    f"Webhook {tnode.value}: {http_method.value} /{webhook_path}"
                )
                if prop_names or redacted_names or redacted or auth_id:
                    lines = ["## Reconnect manually", f"Webhook node: {tnode.value}"]
                    if auth_id:
                        lines.append(f"authProvisionId: {auth_id}")
                    for pname in redacted_names:
                        lines.append(f"Redacted prop: {pname}")
                    for pname in prop_names:
                        lines.append(f"Unrecognized prop: {pname}")
                    sticky_content = "\n".join(lines)
                    add_sticky(tnode, sticky_content)
                    manual.append(
                        ManualItem(
                            node_name=tnode.value,
                            headline="Reconnect manually: review webhook props",
                            component_key=None,
                            version=None,
                            auth_provision_id=auth_id,
                            redacted=redacted,
                            names=prop_names + redacted_names,
                        )
                    )
            case EmailTrigger() | FormTrigger() | SourceTrigger() | UnrecognizedTrigger() as unknown_trig:
                add_chain_node(
                    tnode,
                    "n8n-nodes-base.noOp",
                    1,
                    {},
                    chain=False,
                )
                sticky_content = _unknown_trigger_sticky(tnode, unknown_trig)
                add_sticky(tnode, sticky_content)
                manual.append(
                    ManualItem(
                        node_name=tnode.value,
                        headline="Reconnect manually: replace unsupported trigger",
                        component_key=_trigger_component_key(unknown_trig),
                        version=None,
                        auth_provision_id=_trigger_auth(unknown_trig),
                        redacted=_trigger_redacted(unknown_trig),
                        names=_trigger_prop_names(unknown_trig),
                    )
                )
                mapped.append(f"Placeholder trigger {tnode.value} (NoOp)")
            case _:
                _absurd(trig)

    first_schedule_tz: str | None = None
    for tz in schedule_timezones:
        if tz:
            if first_schedule_tz is None:
                first_schedule_tz = tz
            elif tz != first_schedule_tz:
                notes.append(
                    f"Schedule timezone {tz!r} differs from workflow timezone {first_schedule_tz!r}; not applied to settings"
                )

    for step in workflow.steps:
        snode = step_node_names[step.namespace]
        match step:
            case CodeStep(namespace=ns, language=lang, runtime=runtime, source=source):
                js_code = _rewrite_code(
                    step,
                    step_node_names,
                    first_trigger_node,
                )
                add_chain_node(
                    snode,
                    "n8n-nodes-base.code",
                    2,
                    {
                        "mode": "runOnceForAllItems",
                        "language": "javaScript",
                        "jsCode": js_code,
                    },
                    chain=True,
                )
                mapped.append(f"Code step {snode.value} ({runtime})")
                if lang is not CodeLanguage.JAVASCRIPT:
                    notes.append(
                        f"Step {ns} was not executed as {runtime}; reconnect manually in a Code node"
                    )
                if "defineComponent" in source:
                    notes.append(
                        f"Step {ns}: remove defineComponent wrapper before running in n8n"
                    )
            case ActionStep(namespace=ns, key=key, version=version, auth_provision_id=auth_id, redacted=redacted, prop_names=prop_names, configured=configured):
                add_chain_node(snode, "n8n-nodes-base.noOp", 1, {}, chain=True)
                lines = [
                    "## Reconnect manually",
                    f"Component: {key}",
                ]
                if version:
                    lines.append(f"Version: {version}")
                if auth_id:
                    lines.append(f"authProvisionId: {auth_id}")
                if redacted:
                    lines.append("Props contained <redacted> values.")
                for pname in prop_names:
                    lines.append(f"Prop: {pname}")
                for key_name, value in configured:
                    lines.append(f"{key_name}: {value}")
                add_sticky(snode, "\n".join(lines))
                manual.append(
                    ManualItem(
                        node_name=snode.value,
                        headline="Reconnect manually: registry action not converted",
                        component_key=key,
                        version=version,
                        auth_provision_id=auth_id,
                        redacted=redacted,
                        names=prop_names,
                    )
                )
                mapped.append(f"Registry action {ns} -> NoOp {snode.value}")
            case UnmappedBranch(namespace=dep_id, nested_namespaces=nested):
                add_chain_node(snode, "n8n-nodes-base.noOp", 1, {}, chain=True)
                nested_text = ", ".join(nested) if nested else "(unknown branches)"
                sticky = (
                    "## Reconnect manually\n"
                    f"Deployment {dep_id}: control flow was not converted.\n"
                    f"Nested steps: {nested_text}"
                )
                add_sticky(snode, sticky)
                manual.append(
                    ManualItem(
                        node_name=snode.value,
                        headline="Reconnect manually: deployment branch not converted",
                        component_key=None,
                        version=None,
                        auth_provision_id=None,
                        redacted=False,
                        names=nested,
                    )
                )
                notes.append(
                    f"Deployment {dep_id} branches were not converted to n8n control flow"
                )
            case UnrecognizedStep(namespace=ns, auth_provision_id=auth_id, redacted=redacted, prop_names=prop_names):
                add_chain_node(snode, "n8n-nodes-base.noOp", 1, {}, chain=True)
                lines = [
                    "## Reconnect manually",
                    f"Unrecognized step: {ns}",
                ]
                if auth_id:
                    lines.append(f"authProvisionId: {auth_id}")
                for pname in prop_names:
                    lines.append(f"Prop: {pname}")
                add_sticky(snode, "\n".join(lines))
                manual.append(
                    ManualItem(
                        node_name=snode.value,
                        headline="Reconnect manually: unrecognized step",
                        component_key=None,
                        version=None,
                        auth_provision_id=auth_id,
                        redacted=redacted,
                        names=prop_names,
                    )
                )
            case _:
                _absurd(step)

    connections: dict[str, dict[str, list[list[N8nConnection]]]] = {}
    if step_chain:
        first_step = step_chain[0]
        for tnode in trigger_node_names:
            connections[tnode.value] = {
                "main": [[{"node": first_step.value, "type": "main", "index": 0}]]
            }
        for i in range(len(step_chain) - 1):
            src = step_chain[i]
            dst = step_chain[i + 1]
            connections[src.value] = {
                "main": [[{"node": dst.value, "type": "main", "index": 0}]]
            }

    settings: dict[str, str] = {"executionOrder": "v1"}
    if first_schedule_tz:
        settings["timezone"] = first_schedule_tz

    document: N8nDocument = {
        "name": workflow.name,
        "nodes": nodes,
        "connections": connections,
        "settings": settings,
        "pinData": {},
    }
    return Conversion(
        document=document,
        report=Report(
            mapped=tuple(mapped),
            manual=tuple(manual),
            notes=tuple(notes),
        ),
    )


def _interval_parameters(seconds: int) -> dict[str, Any]:
    if seconds % 86400 == 0:
        return {
            "field": "days",
            "daysInterval": seconds // 86400,
            "triggerAtHour": 0,
            "triggerAtMinute": 0,
        }
    if seconds % 3600 == 0:
        return {
            "field": "hours",
            "hoursInterval": seconds // 3600,
            "triggerAtMinute": 0,
        }
    if seconds % 60 == 0:
        return {
            "field": "minutes",
            "minutesInterval": seconds // 60,
        }
    return {"field": "seconds", "secondsInterval": seconds}


def _slugify(name: str) -> str:
    slug = re.sub(r"[^a-z0-9]+", "-", name.lower()).strip("-")
    return slug or "webhook"


def _unknown_trigger_sticky(tnode: _NodeName, trig: Trigger) -> str:
    lines = ["## Reconnect manually", f"Trigger node: {tnode.value}"]
    if isinstance(trig, UnrecognizedTrigger):
        lines.append(f"Type: {trig.wire_type}")
    elif isinstance(trig, EmailTrigger):
        lines.append("Type: email")
    elif isinstance(trig, FormTrigger):
        lines.append("Type: form")
    elif isinstance(trig, SourceTrigger):
        lines.append("Type: source")
        if trig.component_key:
            lines.append(f"Component: {trig.component_key}")
    auth = _trigger_auth(trig)
    if auth:
        lines.append(f"authProvisionId: {auth}")
    for pname in _trigger_prop_names(trig):
        lines.append(f"Prop: {pname}")
    return "\n".join(lines)


def _trigger_auth(trig: Trigger) -> str | None:
    match trig:
        case EmailTrigger(auth_provision_id=aid) | FormTrigger(auth_provision_id=aid) | SourceTrigger(auth_provision_id=aid) | UnrecognizedTrigger(auth_provision_id=aid):
            return aid
        case _:
            return None


def _trigger_redacted(trig: Trigger) -> bool:
    match trig:
        case EmailTrigger(redacted=r) | FormTrigger(redacted=r) | SourceTrigger(redacted=r) | UnrecognizedTrigger(redacted=r):
            return r
        case _:
            return False


def _trigger_prop_names(trig: Trigger) -> tuple[str, ...]:
    match trig:
        case EmailTrigger(prop_names=p) | FormTrigger(prop_names=p) | SourceTrigger(prop_names=p) | UnrecognizedTrigger(prop_names=p):
            return p
        case _:
            return ()


def _trigger_component_key(trig: Trigger) -> str | None:
    if isinstance(trig, SourceTrigger):
        return trig.component_key
    return None


def _rewrite_code(
    step: CodeStep,
    names: Mapping[str, _NodeName],
    trigger_node: _NodeName | None,
) -> str:
    header_lines = [
        f'// Migrated from Pipedream code step "{step.namespace}" ({step.runtime}).',
        "// The Pipedream SDK is not available in this Code node.",
    ]
    trig_name = trigger_node.value if trigger_node else "trigger"
    header_lines.append(
        f"// steps.trigger.event -> $('{trig_name}').item.json"
    )
    for ns in sorted(names):
        mapped = names[ns].value
        header_lines.append(
            f"// steps.{ns}.$return_value -> $('{mapped}').item.json"
        )
    header = "\n".join(header_lines) + "\n"

    if step.language is CodeLanguage.JAVASCRIPT:
        body = step.source
        if trigger_node:
            body = body.replace(
                "steps.trigger.event",
                f"$('{trig_name}').item.json",
            )

        def repl(match: re.Match[str]) -> str:
            ident = match.group(1)
            if ident in names:
                return f"$('{names[ident].value}').item.json"
            return match.group(0)

        body = _STEP_RETURN_RE.sub(repl, body)
        return header + body

    commented = "\n".join(f"// {line}" for line in step.source.splitlines())
    throw_line = (
        "throw new Error('Reconnect this step manually in the n8n Code node.');"
    )
    if commented:
        return f"{header}{commented}\n{throw_line}\n"
    return f"{header}{throw_line}\n"


def _node_id(workflow_name: str, node_name: _NodeName) -> str:
    key = f"{workflow_name}\n{node_name.value}"
    return str(uuid.uuid5(uuid.NAMESPACE_DNS, key))


def _absurd(value: Never) -> NoReturn:
    raise AssertionError(f"Unhandled value: {value!r}")


def format_report(
    sections: Sequence[tuple[str, Report]],
    skipped_exports: Sequence[SkippedExport],
) -> str:
    lines: list[str] = []
    for wf_name, report in sections:
        lines.append(wf_name)
        lines.append("Mapped:")
        if report.mapped:
            for item in report.mapped:
                lines.append(f"  - {item}")
        else:
            lines.append("  (none)")
        lines.append("Manual:")
        if report.manual:
            for item in report.manual:
                lines.append(f"  - {item.headline} ({item.node_name})")
        else:
            lines.append("  (none)")
        lines.append("Notes:")
        if report.notes:
            for item in report.notes:
                lines.append(f"  - {item}")
        else:
            lines.append("  (none)")
        lines.append("")
    if skipped_exports:
        lines.append("Skipped exports:")
        for sk in skipped_exports:
            lines.append(f"  - {sk.name} ({sk.status.value})")
            for w in sk.warnings:
                lines.append(f"      warning: {w}")
        lines.append("")
    return "\n".join(lines).rstrip() + "\n"
