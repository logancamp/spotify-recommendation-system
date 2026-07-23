import streamlit as st # type: ignore
import pandas as pd
from spotipy.oauth2 import SpotifyOAuth # type: ignore
from dotenv import load_dotenv
import os
import sys
import spotipy # type: ignore
import datetime, pytz
import time; 
import requests
import plotly.graph_objects as go
import plotly.express as px

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from db_utils import read_df
from pipeline_runner import run_for_user, all_succeeded, first_failure

MOOD_THRESHOLDS = {
    "Happy":("valence", "recommended_valence", ">=", 0.60),
    "Sad":("valence", "recommended_valence", "<=", 0.40),
    "High Energy":("energy", "recommended_energy", ">=", 0.70),
    "Danceable":("danceability", "recommended_danceability", ">=", 0.65),
    "Acoustic":("acousticness", "recommended_acousticness", ">=", 0.50),
    "Instrumental":("instrumentalness", "recommended_instrumentalness", ">=", 0.50),
    "Live Sounding":("liveness", "recommended_liveness", ">=", 0.30),
    "Speech Heavy":("speechiness", "recommended_speechiness", ">=", 0.10),
    "High Tempo":("tempo", "recommended_tempo", ">=", 120),
}

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
    user_hash = st.session_state.get("spotify_user_hash")
    
    if not token_info or not user_hash:
        st.error("Session missing — please log in again.")
        st.session_state.page_state = "login"
        st.rerun()
        return
    access_token = token_info["access_token"]
    
    try:
        fresh_df = read_df(f"""
            SELECT MAX(rr.created_at) AS last_run
            FROM ranked_recommendations rr
            JOIN users u ON u.user_id = rr.user_id
            WHERE u.spotify_user_hash = '{user_hash}'
        """)
        
        if not fresh_df.empty and fresh_df.iloc[0]["last_run"] is not None:
            last_run = fresh_df.iloc[0]["last_run"]
            
            if last_run.tzinfo is None:
                last_run = last_run.replace(tzinfo=pytz.utc)
            
            age_hours = (datetime.datetime.now(pytz.utc) - last_run).total_seconds() / 3600
            if age_hours < 12:
                mins_ago = int(age_hours * 60)
                st.success(f"✅ Using existing recommendations (last updated {mins_ago} min ago).")
                
                if st.button("Continue to survey →", type="primary"):
                    st.session_state.page_state = "survey"
                    st.rerun()
                st.caption("Or wait a moment — redirecting automatically...")
                time.sleep(2)
                st.session_state.page_state = "survey"
                st.rerun()
                return
            
    except Exception:
        pass
    
    with st.container(border=True):
        st.markdown("**Your city** (for weather-based recommendations)")
        st.caption("We use this to fetch local weather and tune your playlist. Leave blank to use the server's location.")
        city = st.text_input(label="City", placeholder="e.g. Cleveland, OH  or  Miami, FL",
                             key="loading_city", label_visibility="collapsed")
    
    if not st.button("Build my recommendations →", type="primary", use_container_width=True):
        return
    
    with st.status("Running pipeline...", expanded=True) as status_box:
        results = run_for_user(access_token, user_hash, city=city.strip())
        for r in results:
            st.write(f"{'✅' if r['success'] else '❌'} {r['step']}")
        
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
    
    all_features = ["High Tempo", "Happy", "Sad", "High Energy", "Danceable",
                    "Acoustic", "Instrumental", "Live Sounding", "Speech Heavy"]
    
    feature_to_attr = {
        "High Tempo": "high_tempo", "Happy": "happy", "Sad": "sad",
        "High Energy": "high_energy", "Danceable": "danceable", "Acoustic": "acoustic",
        "Instrumental": "instrumental", "Live Sounding": "live_sounding", "Speech Heavy": "speech_heavy",
    }
    
    with st.container(border=True):
        st.markdown("**I want my playlist to be:**")
        want = st.multiselect(label="I want", options=all_features, default=[],
                              placeholder="Pick features you want...", label_visibility="collapsed")
    
    with st.container(border=True):
        st.markdown("**I don't want my playlist to be:**")
        dont_want = st.multiselect(label="I don't want", options=[f for f in all_features if f not in want],
                                   default=[], placeholder="Pick features to avoid...", label_visibility="collapsed")
    
    with st.container(border=True):
        st.markdown("**Temperature**")
        st.caption("Higher values lead to more exotic results.")
        temperature = st.slider(label="Temperature", min_value=0.0, max_value=2.0,
                                step=0.05, value=0.0, label_visibility="collapsed")
    
    with st.container(border=True):
        st.markdown("**Number of songs**")
        st.caption("How many tracks should the playlist contain?")
        num_songs = st.slider(label="Number of songs", min_value=5, max_value=50,
                              step=1, value=15, label_visibility="collapsed")
    
    with st.container(border=True):
        st.markdown("**Playlist name**")
        if "survey_playlist_name" not in st.session_state:
            st.session_state.survey_playlist_name = "My Recommended Playlist"
        st.text_input(label="Playlist name", key="survey_playlist_name", max_chars=100,
                      placeholder="Enter a name for your Spotify playlist...", label_visibility="collapsed")
    st.markdown("")
    
    try:
        weather_df = read_df("SELECT temperature_c, text_description FROM context_inputs ORDER BY fetched_at DESC LIMIT 1;")
        if not weather_df.empty:
            wr = weather_df.iloc[0]
            with st.container(border=True):
                st.markdown("**Current Weather**")
                st.caption(f"🌡️ {wr['text_description'] or 'Unknown'}, {wr['temperature_c']}°C")
                apply_weather = st.toggle("Apply weather to recommendations", value=True,
                                          help="When on, weather conditions influence which tracks are ranked higher.")
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
        if not st.session_state.survey_playlist_name.strip():
            st.session_state.survey_playlist_name = "My Recommended Playlist"
        st.session_state.page_state = "playlist"
        st.rerun()


def save_playlist_to_spotify(df, playlist_name: str = "My Recommended Playlist"):
    token_info = st.session_state.get("token_info")
    
    if not token_info:
        st.error("Session expired — please log in again.")
        return
    
    access_token = token_info["access_token"]
    headers = {"Authorization": f"Bearer {access_token}", "Content-Type": "application/json"}
    resp = requests.post("https://api.spotify.com/v1/me/playlists", headers=headers,
                         json={"name": playlist_name, "description": "Generated by the Spotify Recommendation System", "public": False},
                         timeout=20)
    
    resp.raise_for_status()
    playlist = resp.json()
    playlist_id = playlist["id"]
    playlist_url = playlist.get("external_urls", {}).get("spotify", "")
    uris = [
        f"spotify:track:{tid}"
        for tid in df["spotify_track_id"].tolist()
        if not tid.startswith("INJ")
    ]
    
    for i in range(0, len(uris), 100):
        r = requests.post(f"https://api.spotify.com/v1/playlists/{playlist_id}/items", headers=headers,
                          params={"uris": ",".join(uris[i:i+100])}, timeout=20)
        r.raise_for_status()
        
    return playlist_url


def show_playlist():
    user_hash = st.session_state.get("spotify_user_hash")
    if not user_hash:
        st.error("Session expired. Please log in again.")
        st.session_state.page_state = "login"
        st.rerun()

    want_list   = st.session_state.get("survey_want", [])
    dont_list   = st.session_state.get("survey_dont_want", [])
    survey_temp = st.session_state.get("survey_temperature", 0.0)
    apply_weather = st.session_state.get("apply_weather", False)

    MOOD_SQL_COLS = {
        "Happy": ("af.valence", ">=", 0.60),
        "Sad": ("af.valence", "<=", 0.40),
        "High Energy": ("af.energy", ">=", 0.70),
        "Danceable": ("af.danceability", ">=", 0.65),
        "Acoustic": ("af.acousticness", ">=", 0.50),
        "Instrumental": ("af.instrumentalness", ">=", 0.50),
        "Live Sounding": ("af.liveness", ">=", 0.30),
        "Speech Heavy": ("af.speechiness", ">=", 0.10),
        "High Tempo": ("af.tempo", ">=", 120),
    }

    mood_clauses = []
    relax = survey_temp / 2.0 * 0.4
    for feature, (col, op, thresh) in MOOD_SQL_COLS.items():
        if feature in want_list:
            relaxed = thresh * (1.0 - relax) if op == ">=" else thresh * (1.0 + relax)
            mood_clauses.append(f"{col} {op} {relaxed:.4f}")
        elif feature in dont_list:
            flip_op = "<" if op == ">=" else ">"
            relaxed = thresh * (1.0 + relax) if op == ">=" else thresh * (1.0 - relax)
            mood_clauses.append(f"{col} {flip_op} {relaxed:.4f}")
    mood_where = ("AND " + " AND ".join(mood_clauses)) if mood_clauses else ""

    def build_sql(where_clause):
        return f"""
            SELECT
                rr.spotify_track_id,
                rr.name        AS recommended_track_name,
                rr.primary_artist AS recommended_artist_names,
                rr.final_score,
                rr.cluster_similarity,
                rr.context_score,
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
            {where_clause}
            ORDER BY rr.rank_position
            LIMIT 500;
        """

    MIN_SONGS = 5
    fallback_notice = None
    all_data = read_df(build_sql(mood_where))

    # progressive fallback: if too few results, relax thresholds in 3 steps then drop filters entirely
    if len(all_data) < MIN_SONGS and mood_clauses:
        
        for relax_factor in [0.20, 0.40, 0.60]:
            relaxed_clauses = []
            
            for feature, (col, op, thresh) in MOOD_SQL_COLS.items():
                base_relax = survey_temp / 2.0 * 0.4
                total_relax = base_relax + relax_factor
                
                if feature in want_list:
                    r = thresh * (1.0 - total_relax) if op == ">=" else thresh * (1.0 + total_relax)
                    relaxed_clauses.append(f"{col} {op} {r:.4f}")
                elif feature in dont_list:
                    flip_op = "<" if op == ">=" else ">"
                    r = thresh * (1.0 + total_relax) if op == ">=" else thresh * (1.0 - total_relax)
                    relaxed_clauses.append(f"{col} {flip_op} {r:.4f}")
            
            relaxed_where = "AND " + " AND ".join(relaxed_clauses)
            all_data = read_df(build_sql(relaxed_where))
            if len(all_data) >= MIN_SONGS:
                pct = int(relax_factor * 100)
                fallback_notice = f"⚠️ Not enough songs matched your exact filters — thresholds relaxed by {pct}% to find results."
                break
        else:
            # all relaxation attempts failed — drop filters entirely
            all_data = read_df(build_sql(""))
            if not all_data.empty:
                fallback_notice = "⚠️ No songs matched your mood filters — showing your best overall matches instead."
    
    if "cluster_similarity" not in all_data.columns:
        all_data["cluster_similarity"] = all_data["final_score"]
    
    if all_data.empty:
        st.warning("No recommendations found for your account yet.")
        if st.button("Back to survey!", type="primary"):
            st.session_state.page_state = "survey"
            st.rerun()
        return

    if fallback_notice:
        st.warning(fallback_notice)

    df = all_data.copy()

    FEATURE_MAP = {
        "Happy": {"recommended_valence": +1.0},
        "Sad": {"recommended_valence": -1.0},
        "High Energy": {"recommended_energy": +1.0},
        "Danceable": {"recommended_danceability": +1.0},
        "Acoustic": {"recommended_acousticness": +1.0},
        "Instrumental": {"recommended_instrumentalness": +1.0},
        "Live Sounding":{"recommended_liveness": +1.0},
        "Speech Heavy": {"recommended_speechiness": +1.0},
        "High Tempo": {"recommended_tempo": +1.0},
    }

    base_score = df["cluster_similarity"].fillna(0.0)
    
    if apply_weather:
        if "context_score" in df.columns and df["context_score"].notna().any():
            base_score = 0.7 * base_score + 0.3 * df["context_score"].fillna(0.0)
        else:
            st.caption("⚠️ No weather scores stored — run the pipeline again to get learned weather rankings.")
    
    df["_base_score"] = base_score

    survey_score = pd.Series(0.0, index=df.index)
    for feature, cols in FEATURE_MAP.items():
        for col, w in cols.items():
            
            if col not in df.columns:
                continue
            vals = df[col].astype(float)
            
            if col == "recommended_tempo":
                vals = (vals / 200.0).clip(0.0, 1.0)
            
            if feature in want_list:
                survey_score += w * vals
            elif feature in dont_list:
                survey_score -= w * vals

    if want_list or dont_list:
        bs = df["_base_score"]
        bs_norm = (bs - bs.min()) / (bs.max() - bs.min() + 1e-9)
        ss_norm = (survey_score - survey_score.min()) / (survey_score.max() - survey_score.min() + 1e-9)
        alpha = max(0.0, 1.0 - survey_temp / 2.0)
        df["combined_score"] = alpha * ss_norm + (1 - alpha) * bs_norm
        df = df.sort_values("combined_score", ascending=False).reset_index(drop=True)
    else:
        df = df.sort_values("_base_score", ascending=False).reset_index(drop=True)

    num_songs = st.session_state.get("survey_num_songs", 15)
    df = df.head(num_songs).reset_index(drop=True)

    # store exactly the trimmed playlist so analytics validates the same songs in the same order
    st.session_state["current_playlist_df"] = df[[
        "recommended_track_name", "recommended_artist_names",
        "recommended_valence", "recommended_energy", "recommended_danceability",
        "tempo", "recommended_acousticness", "recommended_instrumentalness",
        "recommended_liveness", "recommended_speechiness",
    ]].rename(columns={"tempo": "recommended_tempo"}).copy()

    feature_cols = {
        "recommended_danceability": "Danceability",
        "recommended_energy": "Energy",
        "recommended_valence": "Valence",
        "recommended_acousticness": "Acousticness",
        "recommended_instrumentalness": "Instrumentalness",
        "recommended_liveness": "Liveness",
        "recommended_speechiness": "Speechiness",
    }

    # radar chart uses raw audio feature values (all naturally 0-1)
    # so they match what's shown in the filter validation table
    radar_df = df.copy()

    st.markdown("""<style>
    .song-row { padding:12px 16px; border-radius:8px; display:flex; align-items:center; gap:12px; }
    .song-row-even { background-color:rgba(255,255,255,0.03); }
    .song-row-odd  { background-color:rgba(255,255,255,0.08); }
    .song-row:hover { background-color:rgba(29,185,84,0.15); }
    .song-number { color:#888; font-size:14px; min-width:24px; text-align:right; }
    .song-title  { font-weight:600; font-size:15px; }
    .song-artist { color:#888; font-size:13px; }
    .new-tag { display:inline-block; margin-left:8px; background:#1DB954; color:#000;
               font-size:10px; font-weight:700; padding:1px 6px; border-radius:4px; vertical-align:middle; }
    .explicit-tag { background-color:#888; color:#000; font-size:10px; font-weight:700;
                    padding:1px 4px; border-radius:2px; margin-left:6px; vertical-align:middle; }
    .song-duration { color:#888; font-size:12px; margin-left:8px; }
    .feature-title  { text-align:center; font-size:18px; font-weight:600; margin-bottom:0; }
    .feature-artist { text-align:center; color:#888; font-size:14px; margin-top:2px; }
    .playlist-header { font-size:26px; font-weight:700; margin-bottom:4px; }
    .playlist-sub { color:#888; font-size:14px; margin-bottom:16px; }
    </style>""", unsafe_allow_html=True)

    if apply_weather:
        try:
            wr = read_df("SELECT temperature_c, text_description FROM context_inputs ORDER BY fetched_at DESC LIMIT 1;").iloc[0]
            st.info(f"🌡️ Weather-influenced ranking: **{wr['text_description'] or ''}**, **{wr['temperature_c']}°C**")
        except Exception:
            pass
    else:
        st.info("🎵 Audio-only ranking applied (weather influence off)")

    pname = st.session_state.get("survey_playlist_name", "My Recommended Playlist")
    st.markdown(f'<p class="playlist-header">{pname}</p>', unsafe_allow_html=True)
    st.markdown(f'<p class="playlist-sub">{len(df)} tracks</p>', unsafe_allow_html=True)

    # catalog coverage and filter strictness metrics
    try:
        total_catalog = read_df(f"""
            SELECT COUNT(*) AS n FROM ranked_recommendations rr
            JOIN users u ON u.user_id = rr.user_id
            WHERE u.spotify_user_hash = '{user_hash}';
        """).iloc[0]["n"]
        
        total_filtered = len(all_data)  # after mood filter, before trim
        coverage_pct = int(len(df) / total_catalog * 100) if total_catalog > 0 else 0
        filter_pct = int(total_filtered / total_catalog * 100) if total_catalog > 0 else 100

        c1, c2, c3 = st.columns(3)
        c1.metric("Songs in playlist", len(df))
        c2.metric("Passed mood filter", f"{total_filtered} / {total_catalog}", help="How many songs from the full catalog passed your mood constraints")
        c3.metric("Filter strictness", f"{100 - filter_pct}% filtered out", help="Higher = more selective mood filtering")
    
    except Exception:
        pass
        st.session_state.selected_song = None

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
                        f'<div class="song-row {parity}"><span class="song-number">{i+1}</span>'
                        f'<div><span class="song-title">{row["recommended_track_name"]}</span>{explicit_tag}{new_tag}<br>'
                        f'<span class="song-artist">{row["recommended_artist_names"]}</span>'
                        f'<span class="song-duration">{duration_str}</span></div></div>',
                        unsafe_allow_html=True)
                
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
                    unsafe_allow_html=True)
                fig = go.Figure(data=go.Scatterpolar(
                    r=values+[values[0]], theta=categories+[categories[0]],
                    fill='toself', line=dict(color='#1DB954', width=2),
                    fillcolor='rgba(29,185,84,0.2)', marker=dict(size=5, color='#1DB954')))
                fig.update_layout(
                    polar=dict(radialaxis=dict(visible=True, range=[0,1], showticklabels=False,
                               gridcolor='rgba(150,150,150,0.2)'),
                               angularaxis=dict(gridcolor='rgba(150,150,150,0.2)'), bgcolor='rgba(0,0,0,0)'),
                    showlegend=False, margin=dict(l=50,r=50,t=30,b=30), height=420,
                    paper_bgcolor='rgba(0,0,0,0)', plot_bgcolor='rgba(0,0,0,0)', font=dict(size=12))
                st.plotly_chart(fig, use_container_width=True)
        else:
            with st.container(border=True, height=550):
                st.markdown("<br>"*8, unsafe_allow_html=True)
                st.markdown("<p style='text-align:center;color:#888;font-size:15px;'>Select a song to view its audio features</p>", unsafe_allow_html=True)

    st.divider()
    st.markdown("### Playlist Stats")
    chart_df = df.copy()
    chart_df["label"] = df["recommended_artist_names"] + " — " + df["recommended_track_name"]
    
    feature_options = {
        "Danceability": "recommended_danceability", "Energy": "recommended_energy",
        "Valence": "recommended_valence", "Acousticness": "recommended_acousticness",
        "Instrumentalness": "recommended_instrumentalness", "Liveness": "recommended_liveness",
        "Speechiness": "recommended_speechiness", "Tempo": "tempo",
    }
    
    ctrl_left, ctrl_right = st.columns([2, 1], gap="large")
    
    with ctrl_left:
        selected_feature = st.selectbox("Feature", options=list(feature_options.keys()), label_visibility="collapsed")
    
    with ctrl_right:
        direction = st.radio("Direction", options=["Highest", "Lowest"], horizontal=True, label_visibility="collapsed")
    
    highest = direction == "Highest"
    col = feature_options[selected_feature]
    deduped = chart_df.sort_values(col, ascending=False).drop_duplicates(subset=["label"]).reset_index(drop=True)
    top10 = (deduped.nlargest(10, col) if highest else deduped.nsmallest(10, col)).sort_values(col, ascending=highest)
    fig_feat = go.Figure(go.Bar(x=top10[col], y=top10["label"], orientation='h', marker_color='#1DB954'))
    
    fig_feat.update_layout(title=f"Top 10 {'Highest' if highest else 'Lowest'} — {selected_feature}",
                           height=400, margin=dict(l=0,r=20,t=40,b=20),
                           paper_bgcolor='rgba(0,0,0,0)', plot_bgcolor='rgba(0,0,0,0)', font=dict(color='white'),
                           xaxis=dict(gridcolor='rgba(150,150,150,0.2)', title=selected_feature),
                           yaxis=dict(gridcolor='rgba(0,0,0,0)'))
    
    st.plotly_chart(fig_feat, use_container_width=True)

    col_artist, col_pca = st.columns(2, gap="large")
    with col_artist:
        ac = chart_df["recommended_artist_names"].value_counts().reset_index()
        ac.columns = ["artist", "count"]
        fig_artist = go.Figure(go.Bar(x=ac["artist"], y=ac["count"], marker_color='#1DB954'))
        fig_artist.update_layout(title="Most Frequent Artists", height=400,
                                 margin=dict(l=20,r=20,t=40,b=100),
                                 paper_bgcolor='rgba(0,0,0,0)', plot_bgcolor='rgba(0,0,0,0)', font=dict(color='white'),
                                 xaxis=dict(gridcolor='rgba(0,0,0,0)', tickangle=-35),
                                 yaxis=dict(gridcolor='rgba(150,150,150,0.2)', title="Tracks", dtick=1))
        st.plotly_chart(fig_artist, use_container_width=True)
    
    with col_pca:
        from sklearn.preprocessing import StandardScaler
        from sklearn.decomposition import PCA
        from sklearn.cluster import KMeans

        pca_features = ["recommended_danceability","recommended_energy","recommended_valence",
                        "recommended_acousticness","recommended_instrumentalness",
                        "recommended_liveness","recommended_speechiness"]

        pca_feature_map = {
            "recommended_danceability": "danceability",
            "recommended_energy": "energy",
            "recommended_valence": "valence",
            "recommended_acousticness": "acousticness",
            "recommended_instrumentalness": "instrumentalness",
            "recommended_liveness": "liveness",
            "recommended_speechiness": "speechiness",
        }

        scaler = StandardScaler()
        scaled = scaler.fit_transform(chart_df[pca_features].fillna(0))
        pca = PCA(n_components=2)
        coords = pca.fit_transform(scaled)
        chart_df["pc1"], chart_df["pc2"] = coords[:,0], coords[:,1]

        fig_pca = go.Figure(go.Scatter(
            x=chart_df["pc1"], y=chart_df["pc2"], mode="markers+text",
            name="Songs",
            text=chart_df["recommended_track_name"], textposition="top center",
            textfont=dict(size=9, color="rgba(255,255,255,0.6)"),
            marker=dict(size=10, color='#1DB954', opacity=0.85),
            hovertemplate="<b>%{text}</b><br>%{customdata}<extra></extra>",
            customdata=chart_df["recommended_artist_names"]))

        # load centroids from CSV and project into same PCA space
        try:
            import glob
            centroid_files = glob.glob("data/*_audio_cluster_centroids_db.csv")
            if centroid_files:
                cent_df = pd.read_csv(centroid_files[0])
                
                # map centroid column names to match pca_features
                cent_renamed = cent_df.rename(columns={v: k for k, v in pca_feature_map.items()})
                cent_cols = [c for c in pca_features if c in cent_renamed.columns]
                if cent_cols and len(cent_cols) == len(pca_features):
                    cent_scaled = scaler.transform(cent_renamed[pca_features].fillna(0))
                    cent_coords = pca.transform(cent_scaled)
                    centroid_labels = [f"Cluster {int(row['cluster_id'])}<br>energy={row['energy']:.2f} valence={row['valence']:.2f}"
                                       for _, row in cent_df.iterrows()]
                    
                    fig_pca.add_trace(go.Scatter(
                        x=cent_coords[:,0], y=cent_coords[:,1],
                        mode="markers+text",
                        name="Taste Centroids",
                        text=[f"C{int(r['cluster_id'])}" for _, r in cent_df.iterrows()],
                        textposition="bottom center",
                        textfont=dict(size=11, color="#FF6B35"),
                        marker=dict(size=18, color="#FF6B35", symbol="star",
                                    line=dict(color="white", width=1.5)),
                        hovertemplate="<b>%{customdata}</b><extra></extra>",
                        customdata=centroid_labels,
                    ))
        
        except Exception:
            pass

        fig_pca.update_layout(
            title="Song Similarity Map (PCA) — ⭐ = Taste Centroids",
            height=400, margin=dict(l=20,r=20,t=40,b=20),
            paper_bgcolor='rgba(0,0,0,0)', plot_bgcolor='rgba(0,0,0,0)',
            font=dict(color='white'),
            legend=dict(orientation="h", y=-0.2),
            xaxis=dict(title="PC1", gridcolor='rgba(150,150,150,0.15)', zeroline=False),
            yaxis=dict(title="PC2", gridcolor='rgba(150,150,150,0.15)', zeroline=False))
        st.plotly_chart(fig_pca, use_container_width=True)

    st.divider()
    col_save, col_back = st.columns([3, 1], gap="small")
    with col_save:
        if st.button("💚 Save Playlist to Spotify", type="primary", use_container_width=True):
            with st.spinner("Creating playlist on Spotify..."):
                try:
                    playlist_url = save_playlist_to_spotify(df, playlist_name=pname)
                    st.session_state.survey_playlist_name = "My Recommended Playlist"
                    if playlist_url:
                        st.success(f"✅ **{pname}** saved! [Open in Spotify]({playlist_url})")
                    else:
                        st.success(f"✅ **{pname}** saved!")
                except Exception as e:
                    st.error(f"Failed to create playlist: {e}")
    
    with col_back:
        if st.button("Back to survey", use_container_width=True):
            st.session_state.page_state = "survey"
            st.rerun()

    st.divider()
    show_analytics()


def show_analytics():
    user_hash = st.session_state.get("spotify_user_hash")
    if not user_hash:
        return

    st.markdown("## 📊 Analytics")

    # Weather influence
    with st.expander("🌤️ Weather influence — how today's weather shaped your recommendations", expanded=False):
        try:
            weather_df = read_df("SELECT temperature_c, relative_humidity, wind_speed_m_s, text_description, fetched_at "
                                 "FROM context_inputs ORDER BY fetched_at DESC LIMIT 1;")
            
            if weather_df.empty:
                st.caption("No weather data recorded yet.")
            else:
                wr = weather_df.iloc[0]
                c1,c2,c3,c4 = st.columns(4)
                c1.metric("Condition", str(wr["text_description"] or "Unknown").title())
                c2.metric("Temperature", f"{wr['temperature_c']}°C")
                c3.metric("Humidity", f"{wr['relative_humidity']}%" if wr["relative_humidity"] else "—")
                c4.metric("Wind", f"{wr['wind_speed_m_s']} m/s" if wr["wind_speed_m_s"] else "—")
                score_df = read_df(f"""
                    SELECT rr.name AS track, rr.primary_artist AS artist,
                           rr.cluster_similarity, rr.context_score, rr.final_score,
                           af.energy, af.valence, af.acousticness, af.danceability, af.tempo
                    FROM ranked_recommendations rr
                    JOIN users u ON u.user_id = rr.user_id
                    JOIN audio_features af ON af.spotify_track_id = rr.spotify_track_id
                    WHERE u.spotify_user_hash = '{user_hash}'
                    ORDER BY rr.rank_position;
                """)
                
                if not score_df.empty and score_df["context_score"].notna().any() and score_df["context_score"].sum() > 0:
                    top5 = score_df.nlargest(5, "context_score")[["track","artist","context_score","energy","valence","acousticness"]]
                    bot5 = score_df.nsmallest(5, "context_score")[["track","artist","context_score","energy","valence","acousticness"]]
                    st.markdown("**Top 5 songs most suited to today's weather:**")
                    st.dataframe(top5.style.format({"context_score":"{:.3f}","energy":"{:.2f}","valence":"{:.2f}","acousticness":"{:.2f}"}), use_container_width=True, hide_index=True)
                    st.markdown("**Bottom 5 songs least suited to today's weather:**")
                    st.dataframe(bot5.style.format({"context_score":"{:.3f}","energy":"{:.2f}","valence":"{:.2f}","acousticness":"{:.2f}"}), use_container_width=True, hide_index=True)
                    compare = pd.DataFrame({"Top 5 (weather match)": top5[["energy","valence","acousticness"]].mean(),
                                            "Bottom 5 (weather mismatch)": bot5[["energy","valence","acousticness"]].mean()})
                    st.markdown("**Audio profile comparison:**")
                    st.dataframe(compare.style.format("{:.2f}"), use_container_width=True)
                else:
                    st.caption("Weather scores are all zero — need 5+ recently played tracks matched to weather readings.")
        
        except Exception as e:
            st.caption(f"Could not load weather analytics: {e}")

    # Filter validation — uses exact playlist songs in playlist order 
    with st.expander("🎯 Filter validation — does the playlist match your mood?", expanded=False):
        want_list = st.session_state.get("survey_want", [])
        dont_list = st.session_state.get("survey_dont_want", [])
        
        if not want_list and not dont_list:
            st.caption("No mood filters selected — run the survey first.")
        else:
            playlist_df = st.session_state.get("current_playlist_df")
            
            if playlist_df is None or playlist_df.empty:
                st.caption("Generate a playlist first to see validation.")
            else:
                # rename recommended_* columns to plain names for MOOD_THRESHOLDS lookup
                val_df = playlist_df.rename(columns={
                    "recommended_track_name": "track",
                    "recommended_artist_names": "artist",
                    "recommended_valence": "valence",
                    "recommended_energy": "energy",
                    "recommended_danceability": "danceability",
                    "recommended_tempo": "tempo",
                    "recommended_acousticness": "acousticness",
                    "recommended_instrumentalness": "instrumentalness",
                    "recommended_liveness": "liveness",
                    "recommended_speechiness": "speechiness",
                })
                
                for feature in want_list + dont_list:
                    if feature not in MOOD_THRESHOLDS:
                        continue
                    
                    analytics_col, _, op, thresh = MOOD_THRESHOLDS[feature]
                    if analytics_col not in val_df.columns:
                        continue
                    
                    total = len(val_df)
                    passing = (val_df[analytics_col] >= thresh).sum() if op == ">=" else (val_df[analytics_col] <= thresh).sum()
                    is_want = feature in want_list
                    match_score = (passing / total) if is_want else (1.0 - passing / total)
                    pct = int(match_score * 100)
                    color = "#1DB954" if match_score >= 0.6 else "#e05c5c"
                    direction = "want" if is_want else "don't want"
                    
                    st.markdown(f"""
                    <div style="margin-bottom:10px;">
                        <div style="display:flex; justify-content:space-between; margin-bottom:4px;">
                            <span style="font-size:14px; font-weight:600; color:white;">{feature}
                                <span style="font-weight:400; color:#aaa; font-size:12px;">({direction})</span>
                            </span>
                            <span style="font-size:14px; font-weight:700; color:{color};">{pct}%</span>
                        </div>
                        <div style="background:#2a2a2a; border-radius:6px; height:8px; width:100%;">
                            <div style="background:{color}; width:{pct}%; height:8px; border-radius:6px;"></div>
                        </div>
                    </div>
                    """, unsafe_allow_html=True)
                
                display_cols = [c for c in ["track","artist","valence","energy","danceability","tempo"] if c in val_df.columns]
                st.dataframe(val_df[display_cols].style.format(
                    {"valence":"{:.2f}","energy":"{:.2f}","danceability":"{:.2f}","tempo":"{:.0f}"}),
                    use_container_width=True, hide_index=True)

    # Top track taste drift
    with st.expander("📈 Top track taste drift over time"):
        try:
            hist_df = read_df(f"""
                SELECT s.snapshot_date, s.time_range,
                       AVG(af.energy) AS avg_energy, AVG(af.valence) AS avg_valence,
                       AVG(af.danceability) AS avg_danceability
                FROM user_top_track_snapshots s
                JOIN users u ON u.user_id = s.user_id
                JOIN audio_features af ON af.spotify_track_id = s.spotify_track_id
                WHERE u.spotify_user_hash = '{user_hash}'
                GROUP BY s.snapshot_date, s.time_range ORDER BY s.snapshot_date, s.time_range;
            """)
            
            if hist_df.empty or len(hist_df["snapshot_date"].unique()) < 2:
                st.caption("Run the pipeline on multiple days to see taste drift here.")
            else:
                for tr in ["short_term","medium_term","long_term"]:
                    subset = hist_df[hist_df["time_range"] == tr]
                    
                    if subset.empty: continue
                    st.markdown(f"**{tr.replace('_',' ').title()}**")
                    fig = px.line(subset, x="snapshot_date", y=["avg_energy","avg_valence","avg_danceability"],
                                  labels={"value":"Score (0–1)","snapshot_date":"Date","variable":"Feature"},
                                  color_discrete_map={"avg_energy":"#1DB954","avg_valence":"#FFA500","avg_danceability":"#1E90FF"})
                    fig.update_layout(height=220, margin=dict(l=0,r=0,t=10,b=0),
                                      paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
                                      font=dict(size=11), legend=dict(orientation="h",y=-0.35))
                    st.plotly_chart(fig, use_container_width=True)
        
        except Exception as e:
            st.caption(f"Could not load top track history: {e}")

    # Recently played trends
    with st.expander("🕐 Recently played listening trends"):
        try:
            rec_df = read_df(f"""
                SELECT DATE(rp.played_at) AS play_date, COUNT(*) AS plays,
                       AVG(af.energy) AS avg_energy, AVG(af.valence) AS avg_valence,
                       AVG(af.danceability) AS avg_danceability
                FROM user_recently_played rp
                JOIN users u ON u.user_id = rp.user_id
                JOIN audio_features af ON af.spotify_track_id = rp.spotify_track_id
                WHERE u.spotify_user_hash = '{user_hash}'
                GROUP BY DATE(rp.played_at) ORDER BY play_date DESC LIMIT 30;
            """)
            
            if rec_df.empty:
                st.caption("No recently played data yet.")
            else:
                c1,c2,c3,c4 = st.columns(4)
                c1.metric("Days tracked", len(rec_df))
                c2.metric("Total plays", int(rec_df["plays"].sum()))
                c3.metric("Avg energy", f"{rec_df['avg_energy'].mean():.2f}")
                c4.metric("Avg mood", f"{rec_df['avg_valence'].mean():.2f}")
                
                fig = px.bar(rec_df.sort_values("play_date"), x="play_date", y="plays",
                             labels={"play_date":"Date","plays":"Tracks played"},
                             color_discrete_sequence=["#1DB954"])
                
                fig.update_layout(height=200, margin=dict(l=0,r=0,t=10,b=0),
                                  paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)", font=dict(size=11))
                
                st.plotly_chart(fig, use_container_width=True)
        
        except Exception as e:
            st.caption(f"Could not load recently played data: {e}")

    # Catalog expansion
    with st.expander("🗂️ Catalog — all songs contributing to recommendations"):
        try:
            cat_df = read_df(f"""
                SELECT t.name AS track, t.primary_artist AS artist, ct.seed_type, ct.seed_value,
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
                c1,c2,c3 = st.columns(3)
                c1.metric("Total catalog tracks", len(cat_df))
                c2.metric("Added in last 24h", int(cat_df["is_new"].sum()))
                c3.metric("Seeded from recents", int((cat_df["seed_type"] == "artist_recent").sum()))
                def _highlight(row):
                    return ["background-color:rgba(29,185,84,0.12)"]*len(row) if row["is_new"] else [""]*len(row)
                st.dataframe(
                    cat_df[["track","artist","seed_type","seed_value","added_date","is_new"]]
                    .style.apply(_highlight, axis=1).format({"is_new": lambda x: "🆕" if x else ""}),
                    use_container_width=True, hide_index=True)
        
        except Exception as e:
            st.caption(f"Could not load catalog data: {e}")

    # Pipeline run history
    with st.expander("⚙️ Pipeline run history"):
        try:
            runs_df = read_df(f"""
                SELECT pr.id AS run_id, pr.started_at::date AS run_date, pr.started_at,
                       pr.completed_at, pr.catalog_tracks_added, pr.recommendations_written
                FROM pipeline_runs pr
                JOIN users u ON u.user_id = pr.user_id
                WHERE u.spotify_user_hash = '{user_hash}'
                ORDER BY pr.started_at DESC;
            """)
            
            if runs_df.empty:
                st.caption("No pipeline runs recorded yet.")
            else:
                st.metric("Total pipeline runs", len(runs_df))
                st.dataframe(runs_df.rename(columns={
                    "run_id":"Run ID","run_date":"Date","started_at":"Started",
                    "completed_at":"Completed","catalog_tracks_added":"Catalog Tracks",
                    "recommendations_written":"Recommendations"}),
                    use_container_width=True, hide_index=True)
        
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