from __future__ import annotations

import json
import subprocess
import sys
import tempfile
from pathlib import Path

import pytest

from pd2n8n import load, walk

ROOT = Path(__file__).resolve().parent.parent
EXAMPLES = ROOT / "examples"


def _nodes_by_type(document: dict, ntype: str) -> list[dict]:
    return [n for n in document["nodes"] if n["type"] == ntype]


def _node_named(document: dict, name: str) -> dict:
    for node in document["nodes"]:
        if node["name"] == name:
            return node
    raise AssertionError(f"node {name!r} not found")


def test_cron_code_schedule_and_code_chain() -> None:
    loaded = load(EXAMPLES / "cron-code.json")
    conversion = walk(loaded.workflows[0].workflow)
    doc = conversion.document

    schedule_nodes = _nodes_by_type(doc, "n8n-nodes-base.scheduleTrigger")
    assert len(schedule_nodes) == 1
    interval = schedule_nodes[0]["parameters"]["rule"]["interval"]
    assert interval[0]["expression"] == "0 9 * * 1"

    assert doc["settings"].get("timezone") == "Europe/Warsaw"

    build = _node_named(doc, "build_digest")
    render = _node_named(doc, "render_subject")
    assert "$('trigger').item.json" in build["parameters"]["jsCode"]
    assert "$('build_digest').item.json" in render["parameters"]["jsCode"]

    conns = doc["connections"]
    assert conns["trigger"]["main"][0][0]["node"] == "build_digest"
    assert conns["build_digest"]["main"][0][0]["node"] == "render_subject"


def test_http_code_webhook_and_python_as_comments() -> None:
    loaded = load(EXAMPLES / "http-code.json")
    conversion = walk(loaded.workflows[0].workflow)
    doc = conversion.document

    webhook = _nodes_by_type(doc, "n8n-nodes-base.webhook")[0]
    assert webhook["parameters"]["httpMethod"] == "POST"
    assert webhook["parameters"]["path"] == "inbound-hook"

    code = _node_named(doc, "normalize")
    js = code["parameters"]["jsCode"]
    assert "throw new Error('Reconnect this step manually in the n8n Code node.');" in js
    assert "// def handler(pd):" in js
    assert not any(
        line.strip().startswith("def handler") for line in js.splitlines()
    )


def test_mixed_registry_noop_and_report() -> None:
    loaded = load(EXAMPLES / "mixed.json")
    conversion = walk(loaded.workflows[0].workflow)
    doc = conversion.document

    assert _node_named(doc, "send_slack")["type"] == "n8n-nodes-base.noOp"
    stickies = [
        n for n in doc["nodes"] if n["type"] == "n8n-nodes-base.stickyNote"
    ]
    slack_sticky = next(
        s
        for s in stickies
        if "slack-send-message" in s["parameters"]["content"]
    )
    content = slack_sticky["parameters"]["content"]
    assert "Reconnect manually" in content
    assert "slack-send-message" in content
    assert "apn_example" in content
    assert "channel: #leads" in content

    webhook_sticky = next(
        s for s in stickies if "Redacted prop: api_key" in s["parameters"]["content"]
    )
    assert "Reconnect manually" in webhook_sticky["parameters"]["content"]

    report_text = conversion.report.manual[0].headline.lower()
    assert "reconnect manually" in report_text or any(
        "reconnect manually" in item.headline.lower() for item in conversion.report.manual
    )

    names = {n["name"] for n in doc["nodes"]}
    assert "old_debug" not in names


def test_project_export_zip_cli() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        out = Path(tmp) / "out"
        proc = subprocess.run(
            [
                sys.executable,
                "-m",
                "pd2n8n",
                str(EXAMPLES / "project-export.zip"),
                "--out",
                str(out),
            ],
            capture_output=True,
            text=True,
            cwd=ROOT,
            check=False,
        )
        assert proc.returncode == 0, proc.stderr
        written = sorted(out.glob("*.json"))
        assert len(written) == 2
        assert "Draft" in proc.stdout
        assert "skipped" in proc.stdout.lower()


def test_missing_file_and_manifest_json_exit_one(tmp_path: Path) -> None:
    missing = tmp_path / "nope.json"
    out = tmp_path / "out"
    proc = subprocess.run(
        [sys.executable, "-m", "pd2n8n", str(missing), "--out", str(out)],
        capture_output=True,
        text=True,
        cwd=ROOT,
    )
    assert proc.returncode == 1
    assert not out.exists()

    manifest = tmp_path / "manifest.json"
    manifest.write_text(
        json.dumps({"schema": "project-export/2026.01", "workflows": []}),
        encoding="utf-8",
    )
    proc2 = subprocess.run(
        [sys.executable, "-m", "pd2n8n", str(manifest), "--out", str(out)],
        capture_output=True,
        text=True,
        cwd=ROOT,
    )
    assert proc2.returncode == 1
    assert not out.exists()


def test_cron_conversion_is_idempotent(tmp_path: Path) -> None:
    out = tmp_path / "out"
    cmd = [
        sys.executable,
        "-m",
        "pd2n8n",
        str(EXAMPLES / "cron-code.json"),
        "--out",
        str(out),
    ]
    subprocess.run(cmd, cwd=ROOT, check=True)
    first = (out / "cron-code.json").read_bytes()
    subprocess.run(cmd, cwd=ROOT, check=True)
    second = (out / "cron-code.json").read_bytes()
    assert first == second
