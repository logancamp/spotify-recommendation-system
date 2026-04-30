import streamlit as st # type: ignore
import pandas as pd
from spotipy.oauth2 import SpotifyOAuth # type: ignore
from dotenv import load_dotenv
import os
import sys
import spotipy # type: ignore

# need this so we can import db_utils no matter where we run the script from
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from db_utils import read_df
from pipeline_runner import run_for_user, all_succeeded, first_failure

load_dotenv()

st.set_page_config(layout="wide")

st.title("Spotify Playlist Generator")
st.text("Generate a playlist based on your mood and favorite songs!")
st.divider()

if "page_state" not in st.session_state:
    st.session_state.page_state = "login"

def show_login():
    st.markdown("### Log into spotify!")
    
    oauth = SpotifyOAuth(
        client_id=os.getenv("SPOTIPY_CLIENT_ID"),
        client_secret=os.getenv("SPOTIPY_CLIENT_SECRET"),
        redirect_uri=os.getenv("SPOTIPY_REDIRECT_URI", "http://127.0.0.1:8501"),
        scope="user-top-read user-read-recently-played playlist-modify-public playlist-modify-private",
        cache_path=None
    )

    auth_url = oauth.get_authorize_url()
    st.markdown(f'<a href="{auth_url}" target="_self"><button>Login with Spotify</button></a>', unsafe_allow_html=True)

    code = st.query_params.get("code")

    if code:
        token_info = oauth.get_access_token(code, as_dict=True)
        st.session_state.token_info = token_info
        sp = spotipy.Spotify(auth=token_info["access_token"])
        user = sp.current_user()
        st.session_state.spotify_user_hash = user["id"]
        st.session_state.display_name = user.get("display_name") or user["id"]
        st.query_params.clear()
        st.session_state.page_state = "loading"
        st.rerun()


def show_loading():
    display_name = st.session_state.get("display_name", "there")
    st.markdown(f"### Welcome, {display_name}! Let's build your recommendations.")
    st.caption("This runs once and takes about 1–2 minutes.")

    token_info = st.session_state.get("token_info")
    user_hash  = st.session_state.get("spotify_user_hash")

    if not token_info or not user_hash:
        st.error("Session missing — please log in again.")
        st.session_state.page_state = "login"
        st.rerun()
        return

    access_token = token_info["access_token"]

    # skip re-running the whole pipeline if we already have recommendations from the last 12 hours
    try:
        fresh_df = read_df(f"""
            SELECT MAX(rr.created_at) AS last_run
            FROM ranked_recommendations rr
            JOIN users u ON u.user_id = rr.user_id
            WHERE u.spotify_user_hash = '{user_hash}'
        """)
        if not fresh_df.empty and fresh_df.iloc[0]["last_run"] is not None:
            import datetime, pytz
            last_run = fresh_df.iloc[0]["last_run"]
            if last_run.tzinfo is None:
                last_run = last_run.replace(tzinfo=pytz.utc)
            age_hours = (datetime.datetime.now(pytz.utc) - last_run).total_seconds() / 3600
            if age_hours < 12:  # still fresh, no need to run everything again
                mins_ago = int(age_hours * 60)
                st.success(f"✅ Using existing recommendations (last updated {mins_ago} min ago).")
                if st.button("Continue to survey →", type="primary"):
                    st.session_state.page_state = "survey"
                    st.rerun()
                st.caption("Or wait a moment — redirecting automatically...")
                import time; time.sleep(2)
                st.session_state.page_state = "survey"
                st.rerun()
                return
    except Exception:
        pass  # If check fails, just run the pipeline

    # ask for city before running so weather context is accurate
    with st.container(border=True):
        st.markdown("**Your city** (for weather-based recommendations)")
        st.caption("We use this to fetch local weather and tune your playlist. Leave blank to use the server's location.")
        city = st.text_input(
            label="City",
            placeholder="e.g. Cleveland, OH  or  Miami, FL",
            key="loading_city",
            label_visibility="collapsed",
        )

    if not st.button("Build my recommendations →", type="primary", use_container_width=True):
        return

    with st.status("Running pipeline...", expanded=True) as status_box:
        results = run_for_user(access_token, user_hash, city=city.strip())

        for r in results:
            icon = "✅" if r["success"] else "❌"
            st.write(f"{icon} {r['step']}")

        if all_succeeded(results):
            status_box.update(label="✅ Recommendations ready!", state="complete")
            st.session_state.page_state = "survey"
            st.rerun()
        else:
            fail = first_failure(results)
            status_box.update(label="❌ Pipeline failed", state="error")
            if fail is None:
                st.error("Pipeline failed but could not identify the step.")
            else:
                st.error(f"Step **{fail['step']}** failed.")
                with st.expander("Error details"):
                    st.code(fail["stderr"][-3000:] or fail["stdout"][-3000:])
            if st.button("Retry"):
                st.rerun()
            if st.button("Skip and use existing recommendations"):
                st.session_state.page_state = "survey"
                st.rerun()


def show_survey():
    display_name = st.session_state.get("display_name", "there")
    st.markdown(f"### How is your mood today, {display_name}?")
    st.caption("Tell us what you're feeling and we'll build a playlist for you.")

    all_features = [
        "High Tempo", "Happy", "Sad", "High Energy", "Danceable",
        "Acoustic", "Instrumental", "Live Sounding", "Speech Heavy"
    ]

    feature_to_attr = {
        "High Tempo": "high_tempo",
        "Happy": "happy",
        "Sad": "sad",
        "High Energy": "high_energy",
        "Danceable": "danceable",
        "Acoustic": "acoustic",
        "Instrumental": "instrumental",
        "Live Sounding": "live_sounding",
        "Speech Heavy": "speech_heavy",
    }

    with st.container(border=True):
        st.markdown("**I want my playlist to be:**")
        want = st.multiselect(
            label="I want",
            options=all_features,
            default=[],
            placeholder="Pick features you want...",
            label_visibility="collapsed"
        )

    with st.container(border=True):
        st.markdown("**I don't want my playlist to be:**")
        dont_want = st.multiselect(
            label="I don't want",
            options=[f for f in all_features if f not in want],
            default=[],
            placeholder="Pick features to avoid...",
            label_visibility="collapsed"
        )

    with st.container(border=True):
        st.markdown("**Temperature**")
        st.caption("Higher values lead to more exotic results.")
        temperature = st.slider(
            label="Temperature",
            min_value=0.0,
            max_value=2.0,
            step=0.05,
            value=0.0,
            label_visibility="collapsed"
        )

    with st.container(border=True):
        st.markdown("**Number of songs**")
        st.caption("How many tracks should the playlist contain?")
        num_songs = st.slider(
            label="Number of songs",
            min_value=5,
            max_value=50,
            step=1,
            value=15,
            label_visibility="collapsed"
        )

    with st.container(border=True):
        st.markdown("**Playlist name**")
        if "survey_playlist_name" not in st.session_state:
            st.session_state.survey_playlist_name = "My Recommended Playlist"
        playlist_name = st.text_input(
            label="Playlist name",
            key="survey_playlist_name",
            max_chars=100,
            placeholder="Enter a name for your Spotify playlist...",
            label_visibility="collapsed"
        )

    # build the survey_data dict to store what the user selected
    survey_data = {
        "attribute": list(feature_to_attr.values()) + ["temperature"],
        "choice": []
    }
    for feature in all_features:
        if feature in want:
            survey_data["choice"].append("yes")
        elif feature in dont_want:
            survey_data["choice"].append("no")
        else:
            survey_data["choice"].append(None)
    survey_data["choice"].append(temperature)

    st.markdown("")

    # show the current weather and let the user decide if they want it to affect recommendations
    try:
        weather_df = read_df(
            "SELECT temperature_c, text_description FROM context_inputs ORDER BY fetched_at DESC LIMIT 1;"
        )
        if not weather_df.empty:
            wr = weather_df.iloc[0]
            temp = wr["temperature_c"]
            desc = wr["text_description"] or "Unknown"
            with st.container(border=True):
                st.markdown("**Current Weather**")
                st.caption(f"🌡️ {desc}, {temp}°C")
                apply_weather = st.toggle(
                    "Apply weather to recommendations",
                    value=True,
                    help="When on, the current weather conditions influence which tracks are ranked higher."
                )
                st.session_state.apply_weather = apply_weather
        else:
            st.session_state.apply_weather = False
    except Exception:
        st.session_state.apply_weather = False

    if st.button("Generate Playlist!", type="primary", use_container_width=True):
        st.session_state.survey_want = want
        st.session_state.survey_dont_want = dont_want
        st.session_state.survey_temperature = temperature
        st.session_state.survey_num_songs = num_songs
        # survey_playlist_name is already in session state via key= binding
        if not st.session_state.survey_playlist_name.strip():
            st.session_state.survey_playlist_name = "My Recommended Playlist"
        st.session_state.page_state = "playlist"
        st.rerun()

def save_playlist_to_spotify(df, playlist_name: str = "My Recommended Playlist"):
    """Create a playlist on the user's Spotify account with the ranked tracks."""
    import requests

    token_info = st.session_state.get("token_info")
    if not token_info:
        st.error("Session expired — please log in again.")
        return

    access_token = token_info["access_token"]
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json",
    }

    # first create the empty playlist, then add the tracks to it
    resp = requests.post(
        "https://api.spotify.com/v1/me/playlists",
        headers=headers,
        json={
            "name": playlist_name,
            "description": "Generated by the Spotify Recommendation System",
            "public": False,
        },
        timeout=20,
    )
    resp.raise_for_status()
    playlist = resp.json()
    playlist_id = playlist["id"]
    playlist_url = playlist.get("external_urls", {}).get("spotify", "")

    # spotify api only allows 100 tracks per request so we chunk it
    uris = [f"spotify:track:{tid}" for tid in df["spotify_track_id"].tolist()]
    for i in range(0, len(uris), 100):
        batch = uris[i:i + 100]
        r = requests.post(
            f"https://api.spotify.com/v1/playlists/{playlist_id}/items",
            headers=headers,
            params={"uris": ",".join(batch)},
            timeout=20,
        )
        r.raise_for_status()

    return playlist_url


def show_playlist():
    import plotly.graph_objects as go

    user_hash = st.session_state.get("spotify_user_hash")
    if not user_hash:
        st.error("Session expired. Please log in again.")
        st.session_state.page_state = "login"
        st.rerun()

    sql = f"""
        SELECT
            rr.spotify_track_id,
            rr.name        AS recommended_track_name,
            rr.primary_artist AS recommended_artist_names,
            rr.final_score,
            rr.cluster_similarity,
            rr.rank_position,
            af.danceability  AS recommended_danceability,
            af.energy        AS recommended_energy,
            af.valence       AS recommended_valence,
            af.acousticness  AS recommended_acousticness,
            af.instrumentalness AS recommended_instrumentalness,
            af.liveness      AS recommended_liveness,
            af.speechiness   AS recommended_speechiness,
            af.tempo,
            af.loudness,
            t.popularity,
            t.duration_ms,
            t.explicit,
            -- mark tracks whose catalog entry was stamped with the latest pipeline run as new
            CASE WHEN ct.pipeline_run_id = (
                SELECT MAX(id) FROM pipeline_runs
                WHERE user_id = (SELECT user_id FROM users WHERE spotify_user_hash = '{user_hash}')
            ) AND (
                SELECT COUNT(*) FROM pipeline_runs
                WHERE user_id = (SELECT user_id FROM users WHERE spotify_user_hash = '{user_hash}')
            ) > 1
            THEN true ELSE false END AS is_new
        FROM ranked_recommendations rr
        JOIN users u ON u.user_id = rr.user_id
        JOIN audio_features af ON af.spotify_track_id = rr.spotify_track_id
        JOIN tracks t ON t.spotify_track_id = rr.spotify_track_id
        LEFT JOIN (
            SELECT spotify_track_id, MIN(pulled_at) AS first_added, MAX(pipeline_run_id) AS pipeline_run_id
            FROM catalog_tracks
            WHERE user_id = (SELECT user_id FROM users WHERE spotify_user_hash = '{user_hash}')
            GROUP BY spotify_track_id
        ) ct ON ct.spotify_track_id = rr.spotify_track_id
        WHERE u.spotify_user_hash = '{user_hash}'
        ORDER BY rr.rank_position
        LIMIT 120;
    """
    all_data = read_df(sql)
    
    if "cluster_similarity" not in all_data.columns:
        all_data["cluster_similarity"] = all_data["final_score"]

    if all_data.empty:
        st.warning(
            "No recommendations found for your account yet. "
            "Ask the project owner to run the pipeline for your Spotify user."
        )
        if st.button("Back to survey!", type="primary"):
            st.session_state.page_state = "survey"
            st.rerun()
        return

    df = all_data.copy()

    # re-rank the songs based on what the user selected in the survey
    FEATURE_MAP = {
        "Happy":        {"recommended_valence": +1.0},
        "Sad":          {"recommended_valence": -1.0},
        "High Energy":  {"recommended_energy": +1.0},
        "Danceable":    {"recommended_danceability": +1.0},
        "Acoustic":     {"recommended_acousticness": +1.0},
        "Instrumental": {"recommended_instrumentalness": +1.0},
        "Live Sounding":{"recommended_liveness": +1.0},
        "Speech Heavy": {"recommended_speechiness": +1.0},
        "High Tempo":   {"recommended_tempo": +1.0},
    }
    want_list = st.session_state.get("survey_want", [])
    dont_list = st.session_state.get("survey_dont_want", [])
    survey_temp = st.session_state.get("survey_temperature", 0.0)
    apply_weather = st.session_state.get("apply_weather", False)

    # --- step 1: build the base score (cluster similarity ± weather) ---
    base_score = df["cluster_similarity"].fillna(0.0)

    if apply_weather:
        try:
            weather_df = read_df(
                "SELECT temperature_c, relative_humidity, wind_speed_m_s, text_description "
                "FROM context_inputs ORDER BY fetched_at DESC LIMIT 1;"
            )
            if not weather_df.empty:
                wr = weather_df.iloc[0]
                weather_ctx = {
                    "temperature_c": wr["temperature_c"],
                    "relative_humidity": wr["relative_humidity"],
                    "wind_speed_m_s": wr["wind_speed_m_s"],
                    "text_description": str(wr["text_description"] or "").lower(),
                }
                # mirror context_fit_score from cluster.py
                def _ctx_score(row):
                    s, w = 0.0, 0.0
                    temp = weather_ctx.get("temperature_c")
                    desc = weather_ctx.get("text_description", "")
                    wind = weather_ctx.get("wind_speed_m_s")
                    en = float(row.get("recommended_energy", 0.5) or 0.5)
                    va = float(row.get("recommended_valence", 0.5) or 0.5)
                    da = float(row.get("recommended_danceability", 0.5) or 0.5)
                    ac = float(row.get("recommended_acousticness", 0.5) or 0.5)
                    if (temp is not None and temp < 10) or any(x in desc for x in ["fog","cloud","overcast","mist"]):
                        s += (en + va) / 2; w += 1.0
                    if any(x in desc for x in ["rain","storm","thunder","drizzle","shower"]):
                        s += (ac + (1 - en)) / 2; w += 1.0
                    if temp is not None and temp > 25:
                        s += (da + va) / 2; w += 1.0
                    if wind is not None and wind > 10:
                        s += en; w += 1.0
                    return s / w if w > 0 else 0.5
                ctx_scores = df.apply(_ctx_score, axis=1)
                base_score = 0.7 * base_score + 0.3 * ctx_scores
        except Exception:
            pass  # fall back to pure cluster similarity

    df["_base_score"] = base_score

    # --- step 2: blend in survey preferences via temperature ---
    survey_score = pd.Series(0.0, index=df.index)
    for feature, cols in FEATURE_MAP.items():
        if feature in want_list:
            for col, w in cols.items():
                if col in df.columns:
                    # normalise tempo (0-250 bpm range) to 0-1 before adding
                    vals = df[col].astype(float)
                    if col == "recommended_tempo":
                        vals = (vals / 200.0).clip(0.0, 1.0)
                    survey_score += w * vals
        elif feature in dont_list:
            for col, w in cols.items():
                if col in df.columns:
                    vals = df[col].astype(float)
                    if col == "recommended_tempo":
                        vals = (vals / 200.0).clip(0.0, 1.0)
                    survey_score -= w * vals

    if want_list or dont_list:
        bs = df["_base_score"]
        bs_norm = (bs - bs.min()) / (bs.max() - bs.min() + 1e-9)
        ss_norm = (survey_score - survey_score.min()) / (survey_score.max() - survey_score.min() + 1e-9)
        # temp=0 → alpha=1.0 (pure cluster), temp=2 → alpha=0 (pure survey)
        alpha = max(0.0, 1.0 - survey_temp / 2.0)
        df["combined_score"] = alpha * bs_norm + (1 - alpha) * ss_norm
        df = df.sort_values("combined_score", ascending=False).reset_index(drop=True)
    else:
        # no survey preferences — sort purely by base score
        df = df.sort_values("_base_score", ascending=False).reset_index(drop=True)
    # --- end survey re-ranking ---

    # trim to however many songs the user asked for in the slider
    num_songs = st.session_state.get("survey_num_songs", 15)
    df = df.head(num_songs).reset_index(drop=True)

    # show the weather banner if the user toggled it on (scoring already applied above)
    if apply_weather:
        try:
            weather_df = read_df(
                "SELECT temperature_c, text_description FROM context_inputs ORDER BY fetched_at DESC LIMIT 1;"
            )
            if not weather_df.empty:
                wr = weather_df.iloc[0]
                st.info(f"🌡️ Weather-influenced ranking: **{wr['text_description']}**, **{wr['temperature_c']}°C**")
        except Exception:
            pass
    else:
        st.info("🎵 Audio-only ranking applied (weather influence off)")

    feature_cols = {
        "recommended_danceability": "Danceability",
        "recommended_energy": "Energy",
        "recommended_valence": "Valence",
        "recommended_acousticness": "Acousticness",
        "recommended_instrumentalness": "Instrumentalness",
        "recommended_liveness": "Liveness",
        "recommended_speechiness": "Speechiness",
    }

    # normalized copy used only for the radar chart
    radar_df = df.copy()
    for col in feature_cols:
        col_min = radar_df[col].min()
        col_max = radar_df[col].max()
        if col_max > col_min:
            radar_df[col] = (radar_df[col] - col_min) / (col_max - col_min)
        else:
            radar_df[col] = 0.5

    st.markdown("""
    <style>
    .song-row {
        padding: 12px 16px;
        border-radius: 8px;
        transition: background-color 0.2s;
        display: flex;
        align-items: center;
        gap: 12px;
    }
    .song-row-even { background-color: rgba(255,255,255,0.03); }
    .song-row-odd  { background-color: rgba(255,255,255,0.08); }
    .song-row:hover { background-color: rgba(29,185,84,0.15); }
    .song-number { color: #888; font-size: 14px; min-width: 24px; text-align: right; }
    .song-title  { font-weight: 600; font-size: 15px; }
    .song-artist { color: #888; font-size: 13px; }
    .new-tag {
        display: inline-block; margin-left: 8px;
        background: #1DB954; color: #000;
        font-size: 10px; font-weight: 700; letter-spacing: 0.5px;
        padding: 1px 6px; border-radius: 4px; vertical-align: middle;
    }
    .feature-title  { text-align: center; font-size: 18px; font-weight: 600; margin-bottom: 0; }
    .feature-artist { text-align: center; color: #888; font-size: 14px; margin-top: 2px; }
    .explicit-tag {
        background-color: #888; color: #000; font-size: 10px; font-weight: 700;
        padding: 1px 4px; border-radius: 2px; margin-left: 6px; vertical-align: middle;
    }
    .song-duration { color: #888; font-size: 12px; margin-left: 8px; }
    .playlist-header { font-size: 26px; font-weight: 700; margin-bottom: 4px; }
    .playlist-sub { color: #888; font-size: 14px; margin-bottom: 16px; }
    </style>
    """, unsafe_allow_html=True)

    # weather banner
    if st.session_state.get("apply_weather", False):
        try:
            weather_df = read_df(
                "SELECT temperature_c, text_description FROM context_inputs ORDER BY fetched_at DESC LIMIT 1;"
            )
            if not weather_df.empty:
                wr = weather_df.iloc[0]
                st.info(f"🌡️ Weather-influenced ranking: **{wr['text_description'] or ''}**, **{wr['temperature_c']}°C**")
        except Exception:
            pass
    else:
        st.info("🎵 Audio-only ranking applied (weather influence off)")

    pname = st.session_state.get("survey_playlist_name", "My Recommended Playlist")
    st.markdown(f'<p class="playlist-header">{pname}</p>', unsafe_allow_html=True)
    st.markdown(f'<p class="playlist-sub">{len(df)} tracks</p>', unsafe_allow_html=True)

    if "selected_song" not in st.session_state:
        st.session_state.selected_song = None

    left, right = st.columns([1, 1], gap="large")

    with left:
        with st.container(border=True, height=550):
            for i, row in df.iterrows():
                col1, col2 = st.columns([5, 1], vertical_alignment="center")
                with col1:
                    parity = "song-row-even" if i % 2 == 0 else "song-row-odd"
                    explicit_tag = '<span class="explicit-tag">E</span>' if row.get("explicit") else ""
                    new_tag = '<span class="new-tag">NEW</span>' if row.get("is_new") else ""
                    duration_ms = int(row.get("duration_ms") or 0)
                    duration_str = f'{duration_ms // 60000}:{(duration_ms % 60000) // 1000:02d}'
                    st.markdown(
                        f'<div class="song-row {parity}">'
                        f'<span class="song-number">{i + 1}</span>'
                        f'<div>'
                        f'<span class="song-title">{row["recommended_track_name"]}</span>{explicit_tag}{new_tag}<br>'
                        f'<span class="song-artist">{row["recommended_artist_names"]}</span>'
                        f'<span class="song-duration">{duration_str}</span>'
                        f'</div></div>',
                        unsafe_allow_html=True
                    )
                with col2:
                    if st.button("View", key=f"view_{i}", use_container_width=True):
                        st.session_state.selected_song = i

    with right:
        if st.session_state.selected_song is not None:
            row = radar_df.iloc[st.session_state.selected_song]
            categories = list(feature_cols.values())
            values = [row[col] for col in feature_cols]
            with st.container(border=True, height=550):
                st.markdown(
                    f'<p class="feature-title">{row["recommended_track_name"]}</p>'
                    f'<p class="feature-artist">{row["recommended_artist_names"]}</p>',
                    unsafe_allow_html=True
                )
                fig = go.Figure(data=go.Scatterpolar(
                    r=values + [values[0]],
                    theta=categories + [categories[0]],
                    fill='toself',
                    line=dict(color='#1DB954', width=2),
                    fillcolor='rgba(29, 185, 84, 0.2)',
                    marker=dict(size=5, color='#1DB954')
                ))
                fig.update_layout(
                    polar=dict(
                        radialaxis=dict(visible=True, range=[0, 1], showticklabels=False, gridcolor='rgba(150,150,150,0.2)'),
                        angularaxis=dict(gridcolor='rgba(150,150,150,0.2)'),
                        bgcolor='rgba(0,0,0,0)'
                    ),
                    showlegend=False,
                    margin=dict(l=50, r=50, t=30, b=30),
                    height=420,
                    paper_bgcolor='rgba(0,0,0,0)',
                    plot_bgcolor='rgba(0,0,0,0)',
                    font=dict(size=12)
                )
                st.plotly_chart(fig, use_container_width=True)
        else:
            with st.container(border=True, height=550):
                st.markdown("<br>" * 8, unsafe_allow_html=True)
                st.markdown("<p style='text-align:center;color:#888;font-size:15px;'>Select a song to view its audio features</p>", unsafe_allow_html=True)

    st.divider()
    st.markdown("### Playlist Stats")

    chart_df = df.copy()
    chart_df["label"] = df["recommended_artist_names"] + " — " + df["recommended_track_name"]

    feature_options = {
        "Danceability": "recommended_danceability",
        "Energy": "recommended_energy",
        "Valence": "recommended_valence",
        "Acousticness": "recommended_acousticness",
        "Instrumentalness": "recommended_instrumentalness",
        "Liveness": "recommended_liveness",
        "Speechiness": "recommended_speechiness",
        "Tempo": "tempo",
    }

    ctrl_left, ctrl_right = st.columns([2, 1], gap="large")
    with ctrl_left:
        selected_feature = st.selectbox("Feature", options=list(feature_options.keys()), label_visibility="collapsed")
    with ctrl_right:
        direction = st.radio("Direction", options=["Highest", "Lowest"], horizontal=True, label_visibility="collapsed")

    highest = direction == "Highest"
    col = feature_options[selected_feature]
    deduped = chart_df.sort_values(col, ascending=False).drop_duplicates(subset=["label"]).reset_index(drop=True)
    top10 = deduped.nlargest(10, col) if highest else deduped.nsmallest(10, col)
    top10 = top10.sort_values(col, ascending=highest).reset_index(drop=True)

    fig_feat = go.Figure(go.Bar(
        x=top10[col],
        y=top10["label"],
        orientation='h',
        marker_color='#1DB954'
    ))
    fig_feat.update_layout(
        title=f"Top 10 {'Highest' if highest else 'Lowest'} — {selected_feature}",
        height=400,
        margin=dict(l=0, r=20, t=40, b=20),
        paper_bgcolor='rgba(0,0,0,0)',
        plot_bgcolor='rgba(0,0,0,0)',
        font=dict(color='white'),
        xaxis=dict(gridcolor='rgba(150,150,150,0.2)', title=selected_feature),
        yaxis=dict(gridcolor='rgba(0,0,0,0)')
    )
    st.plotly_chart(fig_feat, use_container_width=True)

    col_artist, col_pca = st.columns(2, gap="large")

    with col_artist:
        artist_counts = (
            chart_df["recommended_artist_names"]
            .value_counts()
            .reset_index()
        )
        artist_counts.columns = ["artist", "count"]
        artist_counts = artist_counts.sort_values("count", ascending=False)

        fig_artist = go.Figure(go.Bar(
            x=artist_counts["artist"],
            y=artist_counts["count"],
            marker_color='#1DB954'
        ))
        fig_artist.update_layout(
            title="Most Frequent Artists",
            height=400,
            margin=dict(l=20, r=20, t=40, b=100),
            paper_bgcolor='rgba(0,0,0,0)',
            plot_bgcolor='rgba(0,0,0,0)',
            font=dict(color='white'),
            xaxis=dict(gridcolor='rgba(0,0,0,0)', tickangle=-35),
            yaxis=dict(gridcolor='rgba(150,150,150,0.2)', title="Tracks", dtick=1)
        )
        st.plotly_chart(fig_artist, use_container_width=True)

    with col_pca:
        from sklearn.preprocessing import StandardScaler
        from sklearn.decomposition import PCA

        pca_features = [
            "recommended_danceability", "recommended_energy", "recommended_valence",
            "recommended_acousticness", "recommended_instrumentalness",
            "recommended_liveness", "recommended_speechiness"
        ]
        pca_df = chart_df[pca_features].fillna(0)
        scaled = StandardScaler().fit_transform(pca_df)
        coords = PCA(n_components=2).fit_transform(scaled)

        chart_df["pc1"] = coords[:, 0]
        chart_df["pc2"] = coords[:, 1]

        fig_pca = go.Figure(go.Scatter(
            x=chart_df["pc1"],
            y=chart_df["pc2"],
            mode="markers+text",
            text=chart_df["recommended_track_name"],
            textposition="top center",
            textfont=dict(size=9, color="rgba(255,255,255,0.6)"),
            marker=dict(size=10, color='#1DB954', opacity=0.85),
            hovertemplate="<b>%{text}</b><br>%{customdata}<extra></extra>",
            customdata=chart_df["recommended_artist_names"]
        ))
        fig_pca.update_layout(
            title="Song Similarity Map (PCA)",
            height=400,
            margin=dict(l=20, r=20, t=40, b=20),
            paper_bgcolor='rgba(0,0,0,0)',
            plot_bgcolor='rgba(0,0,0,0)',
            font=dict(color='white'),
            xaxis=dict(title="PC1", gridcolor='rgba(150,150,150,0.15)', zeroline=False),
            yaxis=dict(title="PC2", gridcolor='rgba(150,150,150,0.15)', zeroline=False)
        )
        st.plotly_chart(fig_pca, use_container_width=True)

    st.divider()
    col_save, col_back = st.columns([3, 1], gap="small")
    with col_save:
        if st.button("💚 Save Playlist to Spotify", type="primary", use_container_width=True):
            with st.spinner("Creating playlist on Spotify..."):
                try:
                    playlist_url = save_playlist_to_spotify(df, playlist_name=pname)
                    st.session_state.survey_playlist_name = "My Recommended Playlist"
                    st.success(f"✅ **{pname}** saved! [Open in Spotify]({playlist_url})")
                except Exception as e:
                    st.error(f"Failed to create playlist: {e}")
    with col_back:
        if st.button("Back to survey", use_container_width=True):
            st.session_state.page_state = "survey"
            st.rerun()

    st.divider()
    show_analytics()


def show_analytics():
    import plotly.express as px

    user_hash = st.session_state.get("spotify_user_hash")
    if not user_hash:
        return

    st.markdown("## 📊 Analytics")

    # ── 1. Filter validation ──────────────────────────────────────────────────
    with st.expander("🎯 Filter validation — does the playlist match your mood?", expanded=True):
        want_list = st.session_state.get("survey_want", [])
        dont_list = st.session_state.get("survey_dont_want", [])

        if not want_list and not dont_list:
            st.caption("No mood filters selected — run the survey first.")
        else:
            try:
                val_df = read_df(f"""
                    SELECT rr.name AS track, rr.primary_artist AS artist,
                           af.valence, af.energy, af.danceability,
                           af.acousticness, af.instrumentalness,
                           af.liveness, af.speechiness, af.tempo
                    FROM ranked_recommendations rr
                    JOIN users u ON u.user_id = rr.user_id
                    JOIN audio_features af ON af.spotify_track_id = rr.spotify_track_id
                    WHERE u.spotify_user_hash = '{user_hash}'
                    ORDER BY rr.rank_position LIMIT 20;
                """)
                if val_df.empty:
                    st.caption("No recommendations found.")
                else:
                    THRESHOLDS = {
                        "Happy":         ("valence",          ">=", 0.60),
                        "Sad":           ("valence",          "<=", 0.40),
                        "High Energy":   ("energy",           ">=", 0.70),
                        "Danceable":     ("danceability",     ">=", 0.65),
                        "Acoustic":      ("acousticness",     ">=", 0.50),
                        "Instrumental":  ("instrumentalness", ">=", 0.50),
                        "Live Sounding": ("liveness",         ">=", 0.30),
                        "Speech Heavy":  ("speechiness",      ">=", 0.10),
                        "High Tempo":    ("tempo",            ">=", 120),
                    }
                    for feature in want_list + dont_list:
                        if feature not in THRESHOLDS:
                            continue
                        col, op, thresh = THRESHOLDS[feature]
                        if col not in val_df.columns:
                            continue
                        total = len(val_df)
                        passing = (val_df[col] >= thresh).sum() if op == ">=" else (val_df[col] <= thresh).sum()
                        pass_rate = passing / total
                        is_want = feature in want_list
                        match_score = pass_rate if is_want else (1.0 - pass_rate)
                        pct = int(match_score * 100)
                        direction = "want" if is_want else "don't want"
                        color = "#1DB954" if match_score >= 0.6 else "#e05c5c"
                        label_color = "#aaa"
                        st.markdown(f"""
                        <div style="margin-bottom:10px;">
                            <div style="display:flex; justify-content:space-between; margin-bottom:4px;">
                                <span style="font-size:14px; font-weight:600; color:white;">{feature} <span style="font-weight:400; color:{label_color}; font-size:12px;">({direction})</span></span>
                                <span style="font-size:14px; font-weight:700; color:{color};">{pct}%</span>
                            </div>
                            <div style="background:#2a2a2a; border-radius:6px; height:8px; width:100%;">
                                <div style="background:{color}; width:{pct}%; height:8px; border-radius:6px; transition:width 0.3s;"></div>
                            </div>
                        </div>
                        """, unsafe_allow_html=True)

                    st.dataframe(
                        val_df[["track", "artist", "valence", "energy", "danceability", "tempo"]].style.format(
                            {"valence": "{:.2f}", "energy": "{:.2f}", "danceability": "{:.2f}", "tempo": "{:.0f}"}
                        ),
                        use_container_width=True, hide_index=True,
                    )
            except Exception as e:
                st.caption(f"Could not load validation data: {e}")

    # ── 2. Top track history ──────────────────────────────────────────────────
    with st.expander("📈 Top track taste drift over time"):
        try:
            hist_df = read_df(f"""
                SELECT s.snapshot_date, s.time_range,
                       AVG(af.energy) AS avg_energy,
                       AVG(af.valence) AS avg_valence,
                       AVG(af.danceability) AS avg_danceability
                FROM user_top_track_snapshots s
                JOIN users u ON u.user_id = s.user_id
                JOIN audio_features af ON af.spotify_track_id = s.spotify_track_id
                WHERE u.spotify_user_hash = '{user_hash}'
                GROUP BY s.snapshot_date, s.time_range
                ORDER BY s.snapshot_date, s.time_range;
            """)
            if hist_df.empty or len(hist_df["snapshot_date"].unique()) < 2:
                st.caption("Run the pipeline on multiple days to see taste drift here.")
            else:
                for tr in ["short_term", "medium_term", "long_term"]:
                    subset = hist_df[hist_df["time_range"] == tr]
                    if subset.empty:
                        continue
                    st.markdown(f"**{tr.replace('_', ' ').title()}**")
                    fig = px.line(
                        subset, x="snapshot_date",
                        y=["avg_energy", "avg_valence", "avg_danceability"],
                        labels={"value": "Score (0–1)", "snapshot_date": "Date", "variable": "Feature"},
                        color_discrete_map={"avg_energy": "#1DB954", "avg_valence": "#FFA500", "avg_danceability": "#1E90FF"},
                    )
                    fig.update_layout(height=220, margin=dict(l=0, r=0, t=10, b=0),
                        paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
                        font=dict(size=11), legend=dict(orientation="h", y=-0.35))
                    st.plotly_chart(fig, use_container_width=True)
        except Exception as e:
            st.caption(f"Could not load top track history: {e}")

    # ── 3. Recently played trends ─────────────────────────────────────────────
    with st.expander("🕐 Recently played listening trends"):
        try:
            rec_df = read_df(f"""
                SELECT DATE(rp.played_at) AS play_date,
                       COUNT(*) AS plays,
                       AVG(af.energy) AS avg_energy,
                       AVG(af.valence) AS avg_valence,
                       AVG(af.danceability) AS avg_danceability
                FROM user_recently_played rp
                JOIN users u ON u.user_id = rp.user_id
                JOIN audio_features af ON af.spotify_track_id = rp.spotify_track_id
                WHERE u.spotify_user_hash = '{user_hash}'
                GROUP BY DATE(rp.played_at)
                ORDER BY play_date DESC LIMIT 30;
            """)
            if rec_df.empty:
                st.caption("No recently played data yet.")
            else:
                c1, c2, c3, c4 = st.columns(4)
                c1.metric("Days tracked", len(rec_df))
                c2.metric("Total plays", int(rec_df["plays"].sum()))
                c3.metric("Avg energy", f"{rec_df['avg_energy'].mean():.2f}")
                c4.metric("Avg mood", f"{rec_df['avg_valence'].mean():.2f}")

                fig = px.bar(rec_df.sort_values("play_date"), x="play_date", y="plays",
                    labels={"play_date": "Date", "plays": "Tracks played"},
                    color_discrete_sequence=["#1DB954"])
                fig.update_layout(height=200, margin=dict(l=0, r=0, t=10, b=0),
                    paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)", font=dict(size=11))
                st.plotly_chart(fig, use_container_width=True)
        except Exception as e:
            st.caption(f"Could not load recently played data: {e}")

    # ── 4. Catalog expansion ─────────────────────────────────────────────────
    with st.expander("🗂️ Catalog — all songs contributing to recommendations"):
        try:
            cat_df = read_df(f"""
                SELECT
                    t.name AS track,
                    t.primary_artist AS artist,
                    ct.seed_type,
                    ct.seed_value,
                    ct.pulled_at::date AS added_date,
                    CASE WHEN ct.pulled_at >= NOW() - INTERVAL '24 hours' THEN true ELSE false END AS is_new
                FROM catalog_tracks ct
                JOIN tracks t ON t.spotify_track_id = ct.spotify_track_id
                JOIN users u ON u.user_id = ct.user_id
                WHERE u.spotify_user_hash = '{user_hash}'
                ORDER BY ct.pulled_at DESC;
            """)
            if cat_df.empty:
                st.caption("Catalog is empty — run the pipeline first.")
            else:
                total = len(cat_df)
                new_count = int(cat_df["is_new"].sum())
                recent_seeded = int((cat_df["seed_type"] == "artist_recent").sum())

                c1, c2, c3 = st.columns(3)
                c1.metric("Total catalog tracks", total)
                c2.metric("Added in last 24h", new_count)
                c3.metric("Seeded from recents", recent_seeded)

                # highlight new rows
                def _highlight(row):
                    return ["background-color: rgba(29,185,84,0.12)"] * len(row) if row["is_new"] else [""] * len(row)

                display = cat_df[["track", "artist", "seed_type", "seed_value", "added_date", "is_new"]]
                st.dataframe(
                    display.style.apply(_highlight, axis=1).format({"is_new": lambda x: "🆕" if x else ""}),
                    use_container_width=True,
                    hide_index=True,
                )
        except Exception as e:
            st.caption(f"Could not load catalog data: {e}")

    # ── 5. Pipeline run history ───────────────────────────────────────────────
    with st.expander("⚙️ Pipeline run history"):
        try:
            runs_df = read_df(f"""
                SELECT
                    pr.id AS run_id,
                    pr.started_at::date AS run_date,
                    pr.started_at,
                    pr.completed_at,
                    pr.catalog_tracks_added,
                    pr.recommendations_written
                FROM pipeline_runs pr
                JOIN users u ON u.user_id = pr.user_id
                WHERE u.spotify_user_hash = '{user_hash}'
                ORDER BY pr.started_at DESC;
            """)
            if runs_df.empty:
                st.caption("No pipeline runs recorded yet.")
            else:
                st.metric("Total pipeline runs", len(runs_df))
                st.dataframe(
                    runs_df.rename(columns={
                        "run_id": "Run ID",
                        "run_date": "Date",
                        "started_at": "Started",
                        "completed_at": "Completed",
                        "catalog_tracks_added": "Catalog Tracks",
                        "recommendations_written": "Recommendations",
                    }),
                    use_container_width=True,
                    hide_index=True,
                )
        except Exception as e:
            st.caption(f"Could not load pipeline history: {e}")


match st.session_state.page_state:
    case "login":
        show_login()
    case "loading":
        show_loading()
    case "survey":
        show_survey()
    case "playlist":
        show_playlist()
