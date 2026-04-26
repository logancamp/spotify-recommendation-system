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

REPO_ROOT = os.path.dirname(os.path.abspath(__file__))
PIPELINE_DIR = os.path.join(REPO_ROOT, "pipeline", "spotify_pipeline")

# use the same python that's running this script so we don't have env issues
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


def run_for_user(access_token: str, user_hash: str, skip_candidates: bool = False, city: str = "") -> list[dict]:
    """
    Run all the pipeline steps for a given user.
    Each step is a subprocess that gets the user's access token injected as an env var.

    Steps:
      1. Ingest top tracks from Spotify into the DB
      2. Enrich those tracks with audio features
      3. Build the candidate pool (search for similar tracks)
      4. Enrich candidate tracks with audio features
      5. Fetch today's weather (uses city if provided, otherwise IP geolocation)
      6. Cluster + rank candidates -> saves to ranked_recommendations

    skip_candidates=True lets you skip steps 3 & 4 if the candidates are already there.
    city is the user's city string (e.g. "Cleveland, OH") for weather context.
    Returns a list of result dicts with step, success, stdout, stderr, returncode.
    """
    extra = {
        "SPOTIFY_ACCESS_TOKEN": access_token,
        "SPOTIFY_USER_HASH": user_hash,
    }
    if city:
        extra["WEATHER_CITY"] = city

    steps = [
        (
            "Ingest top tracks",
            f"{PYTHON} scripts/ingest_spotify_top_tracks.py",
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

    results = []
    for label, cmd, cwd in steps:
        r = _run_step(label, cmd, cwd, extra)
        results.append(r)
        if not r["success"]:
            # stop early - later steps depend on earlier ones working
            break

    return results


def all_succeeded(results: list[dict]) -> bool:
    return all(r["success"] for r in results)


def first_failure(results: list[dict]) -> dict | None:
    for r in results:
        if not r["success"]:
            return r
    return None
