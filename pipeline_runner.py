"""
pipeline_runner.py

Runs the full recommendation pipeline for a user by injecting their
oauth access token as an env var into each subprocess step.

Called by main.py when a user logs in and needs fresh recommendations.
The airflow DAG handles the scheduled nightly runs separately using the refresh token.
"""

import os
import subprocess
import sys
import requests as _req, time as _time

REPO_ROOT = os.path.dirname(os.path.abspath(__file__))
PIPELINE_DIR = os.path.join(REPO_ROOT, "pipeline", "spotify_pipeline")

PYTHON = sys.executable


def _run_step(label: str, cmd: str, cwd: str, extra_env: dict) -> dict:
    env = {**os.environ, **extra_env}
    result = subprocess.run(
        cmd,
        shell=True,
        cwd=cwd,
        env=env,
        capture_output=True,
        text=True,
    )
    success = result.returncode == 0
    if not success:
        print(f"[pipeline_runner] ❌ {label} failed (exit {result.returncode})")
        print(result.stderr[-2000:] if result.stderr else "(no stderr)")
    else:
        print(f"[pipeline_runner] ✅ {label}")
    return {
        "step": label,
        "success": success,
        "stdout": result.stdout,
        "stderr": result.stderr,
        "returncode": result.returncode,
    }


def _create_run_id(user_hash: str) -> str | None:
    """
    Create a pipeline_runs row early so all steps can be stamped with the same run_id.
    Returns the run_id as a string for injection into subprocesses, or None on failure.
    """
    try:
        sys.path.insert(0, REPO_ROOT)
        from db_utils import read_df, create_pipeline_run
        user_row = read_df(f"SELECT user_id FROM users WHERE spotify_user_hash = '{user_hash}' LIMIT 1")
        if user_row.empty:
            return None
        user_id = int(user_row.iloc[0]["user_id"])
        run_id = create_pipeline_run(user_id=user_id)
        print(f"[pipeline_runner] Created pipeline run id={run_id}")
        return str(run_id)
    except Exception as e:
        print(f"[pipeline_runner] WARNING: could not create pipeline run: {e}")
        return None


def run_for_user(access_token: str, user_hash: str, skip_candidates: bool = False, city: str = "") -> list[dict]:
    extra = {
        "SPOTIFY_ACCESS_TOKEN": access_token,
        "SPOTIFY_USER_HASH": user_hash,
    }
    if city:
        extra["WEATHER_CITY"] = city

    # create the run record upfront so catalog + recommendation rows are all stamped with it
    run_id = _create_run_id(user_hash)
    if run_id:
        extra["PIPELINE_RUN_ID"] = run_id

    steps = [
        (
            "Ingest top tracks",
            f"{PYTHON} scripts/ingest_spotify_top_tracks.py",
            PIPELINE_DIR,
        ),
        (
            "Ingest recently played",
            f"{PYTHON} scripts/ingest_recently_played.py",
            PIPELINE_DIR,
        ),
        (
            "Enrich audio features",
            f"{PYTHON} scripts/enrich_audio_features_from_reccobeats.py",
            PIPELINE_DIR,
        ),
    ]

    if not skip_candidates:
        steps += [
            (
                "Build candidate pool",
                f"{PYTHON} scripts/build_catalog_spotify_search.py",
                PIPELINE_DIR,
            ),
            (
                "Enrich candidate features",
                f"{PYTHON} scripts/enrich_catalog_audio_features.py",
                PIPELINE_DIR,
            ),
        ]

    steps.append(
        (
            "Fetch weather",
            f"{PYTHON} data-collect/collect_weather_today.py",
            REPO_ROOT,
        )
    )

    steps.append(
        (
            "Cluster & rank",
            f"{PYTHON} cluster.py",
            REPO_ROOT,
        )
    )


    # Airflow trigger (uncomment to use instead of direct subprocess)
    # Requires AIRFLOW_BASE_URL, AIRFLOW_USERNAME, AIRFLOW_PASSWORD in .env
    # Also needs the 'spotify_pipeline' DAG deployed to the Airflow instance.
    
    # ─────────────────────────────────────────────────────────────────────────────
    # _base = os.getenv("AIRFLOW_BASE_URL", "http://localhost:8080")
    # _resp = _req.post(
    #     f"{_base}/api/v1/dags/spotify_pipeline/dagRuns",
    #     auth=(os.getenv("AIRFLOW_USERNAME") or "", os.getenv("AIRFLOW_PASSWORD") or ""),
    #     json={"conf": {"user_hash": user_hash, 
    #                    "access_token": access_token,
    #                    "city": city, 
    #                    "pipeline_run_id": run_id}}, timeout=10,)
    
    # _resp.raise_for_status()
    # _run_id = _resp.json()["dag_run_id"]
    
    # while True:
    #     _status = _req.get(f"{_base}/api/v1/dags/spotify_pipeline/dagRuns/{_run_id}",
    #                             auth=(os.getenv("AIRFLOW_USERNAME") or "", os.getenv("AIRFLOW_PASSWORD") or ""), timeout=10,).json()
        
    #     if _status["state"] in ("success", "failed"): break
    #     _time.sleep(3)
    
    # return [{"step": "Airflow DAG", 
    #          "success": _status["state"] == "success",
    #          "stdout": "", 
    #          "stderr": "", 
    #          "returncode": 0}]
    # ─────────────────────────────────────────────────────────────────────────────


    results = []
    for label, cmd, cwd in steps:
        r = _run_step(label, cmd, cwd, extra)
        results.append(r)
        if not r["success"]:
            break

    return results


def all_succeeded(results: list[dict]) -> bool:
    return all(r["success"] for r in results)


def first_failure(results: list[dict]) -> dict | None:
    for r in results:
        if not r["success"]:
            return r
    return None
