import streamlit as st
import pandas as pd
from spotipy.oauth2 import SpotifyOAuth
from dotenv import load_dotenv
import os

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
        redirect_uri=os.getenv("SPOTIPY_REDIRECT_URI"),
        scope="user-top-read playlist-modify-public playlist-modify-private",
        cache_path=None
    )

    auth_url = oauth.get_authorize_url()
    st.markdown(f'<a href="{auth_url}" target="_self"><button>Login with Spotify</button></a>', unsafe_allow_html=True)

    code = st.query_params.get("code")

    if code:
        token_info = oauth.get_access_token(code, as_dict=True)
        st.session_state.token_info = token_info
        st.query_params.clear()
        st.session_state.page_state = "survey"
        st.rerun()


def show_survey():
    st.markdown("### How is your mood today, Ethan?")
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

    # Build survey_data in the same format as before
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
    if st.button("Generate Playlist!", type="primary", use_container_width=True):
        print(survey_data)
        st.session_state.page_state = "playlist"
        st.rerun()

def show_playlist():
    import plotly.graph_objects as go

    df = pd.read_csv("track_recommendations_weather_assigned.csv").head(15)

    feature_cols = {
        "recommended_danceability": "Danceability",
        "recommended_energy": "Energy",
        "recommended_valence": "Valence",
        "recommended_acousticness": "Acousticness",
        "recommended_instrumentalness": "Instrumentalness",
        "recommended_liveness": "Liveness",
        "recommended_speechiness": "Speechiness",
    }

    # Normalize z-scored features to 0–1 range
    all_data = pd.read_csv("track_recommendations_weather_assigned.csv")
    for col in feature_cols:
        col_min = all_data[col].min()
        col_max = all_data[col].max()
        df[col] = (df[col] - col_min) / (col_max - col_min)

    st.markdown("### Your Mood Playlist!")

    # Custom CSS for song rows
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

    if st.button("Back to survey!", type="primary"):
        st.session_state.page_state = "survey"
        st.rerun()

match st.session_state.page_state:
    case "login":
        show_login()
    case "survey":
        show_survey()
    case "playlist":
        show_playlist()
