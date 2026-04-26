import streamlit as st
import pandas as pd
from spotipy.oauth2 import SpotifyOAuth
from dotenv import load_dotenv
import os
import sys

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
        scope="user-top-read playlist-modify-public playlist-modify-private",
        cache_path=None
    )

    auth_url = oauth.get_authorize_url()
    st.markdown(f'<a href="{auth_url}" target="_self"><button>Login with Spotify</button></a>', unsafe_allow_html=True)

    code = st.query_params.get("code")

    if code:
        token_info = oauth.get_access_token(code, as_dict=True)
        st.session_state.token_info = token_info
        import spotipy
        sp = spotipy.Spotify(auth=token_info["access_token"])
        user = sp.current_user()
        st.session_state.spotify_user_hash = user["id"]
        st.session_state.display_name = user.get("display_name") or user["id"]
        st.query_params.clear()
        st.session_state.page_state = "loading"
        st.rerun()


def show_loading():
    display_name = st.session_state.get("display_name", "there")
    st.markdown(f"### Welcome, {display_name}! Building your recommendations...")
    st.caption("This runs once per login and takes about 1–2 minutes.")

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

    with st.status("Running pipeline...", expanded=True) as status_box:
        results = run_for_user(access_token, user_hash)

        for r in results:
            icon = "✅" if r["success"] else "❌"
            st.write(f"{icon} {r['step']}")

        if all_succeeded(results):
            status_box.update(label="✅ Recommendations ready!", state="complete")
            st.session_state.page_state = "survey"
            st.rerun()
        else:
            # show which step broke and let the user retry or just skip ahead
            fail = first_failure(results)
            status_box.update(label="❌ Pipeline failed", state="error")
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
            rr.rank_position,
            af.danceability  AS recommended_danceability,
            af.energy        AS recommended_energy,
            af.valence       AS recommended_valence,
            af.acousticness  AS recommended_acousticness,
            af.instrumentalness AS recommended_instrumentalness,
            af.liveness      AS recommended_liveness,
            af.speechiness   AS recommended_speechiness
        FROM ranked_recommendations rr
        JOIN users u ON u.user_id = rr.user_id
        JOIN audio_features af ON af.spotify_track_id = rr.spotify_track_id
        WHERE u.spotify_user_hash = '{user_hash}'
        ORDER BY rr.rank_position
        LIMIT 120;
    """
    all_data = read_df(sql)

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
        "High Tempo":   {"recommended_energy": +0.5, "recommended_danceability": +0.5},
    }
    want_list = st.session_state.get("survey_want", [])
    dont_list = st.session_state.get("survey_dont_want", [])
    survey_temp = st.session_state.get("survey_temperature", 0.0)

    survey_score = pd.Series(0.0, index=df.index)
    for feature, cols in FEATURE_MAP.items():
        if feature in want_list:
            for col, w in cols.items():
                if col in df.columns:
                    survey_score += w * df[col]
        elif feature in dont_list:
            for col, w in cols.items():
                if col in df.columns:
                    survey_score -= w * df[col]

    if want_list or dont_list:
        fs = df["final_score"]
        fs_norm = (fs - fs.min()) / (fs.max() - fs.min() + 1e-9)
        ss_norm = (survey_score - survey_score.min()) / (survey_score.max() - survey_score.min() + 1e-9)
        # alpha controls the mix between cluster score and survey score
        # low temp = trust cluster more, high temp = trust the survey more
        alpha = max(0.0, 1.0 - (survey_temp + 1.0) / 2.0)
        df["combined_score"] = alpha * fs_norm + (1 - alpha) * ss_norm
        df = df.sort_values("combined_score", ascending=False).reset_index(drop=True)
    # --- end survey re-ranking ---

    # trim to however many songs the user asked for in the slider
    num_songs = st.session_state.get("survey_num_songs", 15)
    df = df.head(num_songs).reset_index(drop=True)

    # only show the weather banner if the user toggled it on in the survey
    if st.session_state.get("apply_weather", False):
        try:
            weather_df = read_df(
                "SELECT temperature_c, text_description FROM context_inputs ORDER BY fetched_at DESC LIMIT 1;"
            )
            if not weather_df.empty:
                wr = weather_df.iloc[0]
                temp = wr["temperature_c"]
                desc = wr["text_description"] or ""
                st.info(f"🌡️ Weather-influenced ranking: **{desc}**, **{temp}°C**")
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

    # normalize features to 0-1 so the radar chart doesn't look weird
    for col in feature_cols:
        col_min = df[col].min()
        col_max = df[col].max()
        if col_max > col_min:
            df[col] = (df[col] - col_min) / (col_max - col_min)
        else:
            df[col] = 0.5

    st.markdown("### Your Mood Playlist!")

    # some css styling to make the song list look nicer
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
    .song-row-even {
        background-color: rgba(255, 255, 255, 0.03);
    }
    .song-row-odd {
        background-color: rgba(255, 255, 255, 0.08);
    }
    .song-row:hover {
        background-color: rgba(29, 185, 84, 0.15);
    }
    .song-number {
        color: #888;
        font-size: 14px;
        min-width: 24px;
        text-align: right;
    }
    .song-title {
        font-weight: 600;
        font-size: 15px;
    }
    .song-artist {
        color: #888;
        font-size: 13px;
    }
    .feature-title {
        text-align: center;
        font-size: 18px;
        font-weight: 600;
        margin-bottom: 0;
    }
    .feature-artist {
        text-align: center;
        color: #888;
        font-size: 14px;
        margin-top: 2px;
    }
    </style>
    """, unsafe_allow_html=True)

    if "selected_song" not in st.session_state:
        st.session_state.selected_song = None

    left, right = st.columns([1, 1], gap="large")

    with left:
        with st.container(border=True, height=550):
            for i, row in df.iterrows():
                col1, col2 = st.columns([5, 1], vertical_alignment="center")
                with col1:
                    parity = "song-row-even" if i % 2 == 0 else "song-row-odd"
                    st.markdown(
                        f'<div class="song-row {parity}">'
                        f'<span class="song-number">{i + 1}</span>'
                        f'<div><span class="song-title">{row["recommended_track_name"]}</span><br>'
                        f'<span class="song-artist">{row["recommended_artist_names"]}</span></div>'
                        f'</div>',
                        unsafe_allow_html=True
                    )
                with col2:
                    if st.button("View", key=f"view_{i}", use_container_width=True):
                        st.session_state.selected_song = i

    with right:
        if st.session_state.selected_song is not None:
            row = df.iloc[st.session_state.selected_song]
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
                st.markdown("<p style='text-align:center; color:#888; font-size:15px;'>Select a song to view its audio features</p>", unsafe_allow_html=True)

    st.divider()
    col_save, col_back = st.columns([1, 1], gap="small")
    with col_save:
        if st.button("💚 Save Playlist to Spotify", type="primary", use_container_width=True):
            with st.spinner("Creating playlist on Spotify..."):
                try:
                    pname = st.session_state.get("survey_playlist_name", "My Recommended Playlist")
                    playlist_url = save_playlist_to_spotify(df, playlist_name=pname)
                    st.session_state.survey_playlist_name = "My Recommended Playlist"
                    st.success(f"✅ **{pname}** saved! [Open in Spotify]({playlist_url})")
                except Exception as e:
                    st.error(f"Failed to create playlist: {e}")
    with col_back:
        if st.button("Back to survey!", use_container_width=True):
            st.session_state.page_state = "survey"
            st.rerun()

match st.session_state.page_state:
    case "login":
        show_login()
    case "loading":
        show_loading()
    case "survey":
        show_survey()
    case "playlist":
        show_playlist()
