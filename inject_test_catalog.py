"""
inject_test_catalog.py

Injects a large diverse set of songs into the catalog with estimated audio features.
Covers all mood/feature extremes so filter validation, weather clustering, and
analytics have something meaningful to work with.

Run from the project root:
    python inject_test_catalog.py

Uses the first user in the DB. Safe to re-run (ON CONFLICT DO NOTHING).
"""

import os
import sys
from dotenv import load_dotenv
from sqlalchemy import text

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
load_dotenv()

from db_utils import get_engine

# ---------------------------------------------------------------------------
# Song data: (fake_track_id, name, artist, acousticness, danceability, energy,
#             instrumentalness, liveness, loudness, speechiness, tempo, valence)
# ---------------------------------------------------------------------------
SONGS = [
    # HIGH ENERGY / HIGH DANCEABILITY / HAPPY
    ("INJ001", "Blinding Lights",          "The Weeknd",           0.00, 0.51, 0.80, 0.00, 0.09, -5.9, 0.06, 171, 0.44),
    ("INJ002", "Levitating",               "Dua Lipa",             0.00, 0.70, 0.76, 0.00, 0.12, -4.1, 0.06, 103, 0.82),
    ("INJ003", "Uptown Funk",              "Mark Ronson",          0.05, 0.90, 0.81, 0.00, 0.05, -4.8, 0.15, 115, 0.97),
    ("INJ004", "Can't Stop the Feeling",   "Justin Timberlake",    0.01, 0.89, 0.78, 0.00, 0.11, -4.5, 0.05, 113, 0.96),
    ("INJ005", "Happy",                    "Pharrell Williams",    0.08, 0.83, 0.71, 0.00, 0.10, -5.2, 0.18, 160, 0.96),
    ("INJ006", "Good as Hell",             "Lizzo",                0.04, 0.77, 0.72, 0.00, 0.09, -5.6, 0.08, 96,  0.89),
    ("INJ007", "Shake It Off",             "Taylor Swift",         0.07, 0.64, 0.80, 0.00, 0.13, -4.2, 0.14, 160, 0.89),
    ("INJ008", "Juice",                    "Lizzo",                0.02, 0.84, 0.74, 0.00, 0.08, -4.9, 0.09, 118, 0.91),
    ("INJ009", "As It Was",                "Harry Styles",         0.01, 0.52, 0.73, 0.00, 0.10, -5.3, 0.05, 174, 0.66),
    ("INJ010", "Watermelon Sugar",         "Harry Styles",         0.12, 0.55, 0.76, 0.00, 0.10, -4.8, 0.04, 95,  0.94),
    ("INJ011", "Stay",                     "The Kid LAROI",        0.00, 0.59, 0.84, 0.00, 0.08, -4.7, 0.05, 170, 0.74),
    ("INJ012", "Dynamite",                 "BTS",                  0.03, 0.74, 0.76, 0.00, 0.09, -4.4, 0.07, 114, 0.74),

    # HIGH ENERGY / HIGH TEMPO / INTENSE
    ("INJ013", "Pumped Up Kicks",          "Foster the People",    0.10, 0.58, 0.58, 0.00, 0.08, -6.5, 0.04, 128, 0.72),
    ("INJ014", "Eye of the Tiger",         "Survivor",             0.01, 0.46, 0.88, 0.00, 0.12, -4.8, 0.05, 109, 0.63),
    ("INJ015", "Lose Yourself",            "Eminem",               0.00, 0.33, 0.90, 0.00, 0.13, -4.3, 0.31, 171, 0.41),
    ("INJ016", "Till I Collapse",          "Eminem",               0.00, 0.43, 0.96, 0.00, 0.11, -3.2, 0.28, 171, 0.43),
    ("INJ017", "Thunderstruck",            "AC/DC",                0.00, 0.40, 0.97, 0.00, 0.21, -4.1, 0.04, 133, 0.52),
    ("INJ018", "Jump",                     "Van Halen",            0.03, 0.65, 0.93, 0.00, 0.07, -4.0, 0.04, 130, 0.82),
    ("INJ019", "Mr. Brightside",           "The Killers",          0.00, 0.34, 0.93, 0.00, 0.11, -4.2, 0.05, 148, 0.28),
    ("INJ020", "Highway to Hell",          "AC/DC",                0.01, 0.54, 0.93, 0.00, 0.16, -4.5, 0.05, 116, 0.55),
    ("INJ021", "Enter Sandman",            "Metallica",            0.00, 0.37, 0.95, 0.00, 0.15, -3.8, 0.05, 123, 0.30),
    ("INJ022", "Welcome to the Jungle",    "Guns N Roses",         0.00, 0.41, 0.97, 0.00, 0.14, -3.5, 0.06, 126, 0.46),

    # DANCEABLE / ELECTRONIC / CLUB
    ("INJ023", "One More Time",            "Daft Punk",            0.00, 0.80, 0.79, 0.03, 0.10, -4.6, 0.05, 123, 0.87),
    ("INJ024", "Around the World",         "Daft Punk",            0.00, 0.84, 0.82, 0.08, 0.09, -4.7, 0.04, 121, 0.73),
    ("INJ025", "Blue (Da Ba Dee)",         "Eiffel 65",            0.01, 0.72, 0.92, 0.00, 0.12, -3.8, 0.06, 138, 0.67),
    ("INJ026", "I Feel Love",              "Donna Summer",         0.00, 0.77, 0.78, 0.07, 0.10, -5.4, 0.04, 122, 0.88),
    ("INJ027", "Africa",                   "Toto",                 0.02, 0.55, 0.56, 0.00, 0.08, -7.1, 0.04, 92,  0.82),
    ("INJ028", "Take On Me",               "a-ha",                 0.02, 0.62, 0.74, 0.01, 0.10, -5.0, 0.05, 169, 0.87),
    ("INJ029", "Don't You Want Me",        "Human League",         0.03, 0.67, 0.71, 0.00, 0.09, -6.1, 0.06, 120, 0.80),
    ("INJ030", "Superstition",             "Stevie Wonder",        0.03, 0.79, 0.80, 0.00, 0.09, -5.2, 0.05, 100, 0.82),

    # SAD / LOW VALENCE / EMOTIONAL
    ("INJ031", "Someone Like You",         "Adele",                0.89, 0.22, 0.25, 0.00, 0.12, -9.5, 0.03, 68,  0.18),
    ("INJ032", "The Night We Met",         "Lord Huron",           0.70, 0.33, 0.21, 0.00, 0.10, -10.2, 0.03, 112, 0.10),
    ("INJ033", "Motion Picture Soundtrack","Radiohead",            0.95, 0.18, 0.13, 0.67, 0.09, -14.5, 0.04, 62,  0.07),
    ("INJ034", "Hurt",                     "Nine Inch Nails",      0.82, 0.29, 0.17, 0.00, 0.11, -11.4, 0.05, 75,  0.07),
    ("INJ035", "Mad World",                "Gary Jules",           0.92, 0.41, 0.23, 0.00, 0.09, -10.9, 0.04, 88,  0.13),
    ("INJ036", "Black",                    "Pearl Jam",            0.63, 0.28, 0.44, 0.00, 0.11, -7.6, 0.05, 78,  0.13),
    ("INJ037", "Skinny Love",              "Bon Iver",             0.96, 0.37, 0.32, 0.04, 0.09, -11.0, 0.05, 99,  0.22),
    ("INJ038", "Liability",                "Lorde",                0.91, 0.31, 0.18, 0.00, 0.10, -10.8, 0.03, 101, 0.12),
    ("INJ039", "Fix You",                  "Coldplay",             0.61, 0.33, 0.32, 0.00, 0.11, -8.3, 0.03, 138, 0.24),
    ("INJ040", "Teardrops on My Guitar",   "Taylor Swift",         0.87, 0.46, 0.36, 0.00, 0.08, -8.5, 0.03, 96,  0.36),
    ("INJ041", "When the Party's Over",    "Billie Eilish",        0.93, 0.35, 0.21, 0.00, 0.09, -11.2, 0.04, 92,  0.14),
    ("INJ042", "Everybody Hurts",          "R.E.M.",               0.78, 0.27, 0.29, 0.00, 0.10, -9.8, 0.03, 68,  0.16),

    # ACOUSTIC / CHILL / FOLK
    ("INJ043", "Banana Pancakes",          "Jack Johnson",         0.94, 0.61, 0.39, 0.00, 0.09, -9.2, 0.05, 95,  0.87),
    ("INJ044", "Better Together",          "Jack Johnson",         0.91, 0.57, 0.42, 0.00, 0.08, -9.5, 0.04, 107, 0.86),
    ("INJ045", "Fast Car",                 "Tracy Chapman",        0.88, 0.53, 0.46, 0.00, 0.11, -8.9, 0.04, 100, 0.67),
    ("INJ046", "Blackbird",                "The Beatles",          0.97, 0.48, 0.19, 0.00, 0.09, -13.2, 0.04, 99,  0.58),
    ("INJ047", "The Scientist",            "Coldplay",             0.84, 0.44, 0.30, 0.00, 0.10, -9.7, 0.03, 75,  0.29),
    ("INJ048", "Colorblind",               "Counting Crows",       0.88, 0.31, 0.24, 0.00, 0.09, -11.3, 0.03, 71,  0.27),
    ("INJ049", "Ho Hey",                   "The Lumineers",        0.82, 0.62, 0.55, 0.00, 0.18, -7.2, 0.06, 147, 0.80),
    ("INJ050", "Stubborn Love",            "The Lumineers",        0.79, 0.49, 0.45, 0.00, 0.12, -8.8, 0.05, 133, 0.62),
    ("INJ051", "Holocene",                 "Bon Iver",             0.88, 0.36, 0.27, 0.12, 0.09, -11.5, 0.03, 80,  0.37),
    ("INJ052", "Re: Stacks",               "Bon Iver",             0.95, 0.30, 0.18, 0.31, 0.09, -12.6, 0.03, 68,  0.33),

    # INSTRUMENTAL / AMBIENT
    ("INJ053", "Gymnopédie No. 1",         "Erik Satie",           0.99, 0.21, 0.07, 0.98, 0.09, -18.1, 0.04, 56,  0.36),
    ("INJ054", "Clair de Lune",            "Debussy",              0.99, 0.19, 0.11, 0.99, 0.09, -16.8, 0.04, 78,  0.25),
    ("INJ055", "Experience",               "Ludovico Einaudi",     0.98, 0.24, 0.15, 0.97, 0.10, -15.2, 0.04, 69,  0.21),
    ("INJ056", "Nuvole Bianche",           "Ludovico Einaudi",     0.99, 0.22, 0.13, 0.98, 0.09, -16.0, 0.04, 60,  0.19),
    ("INJ057", "An Ending",                "Brian Eno",            0.91, 0.25, 0.09, 0.99, 0.09, -16.4, 0.04, 62,  0.15),
    ("INJ058", "1/1",                      "Brian Eno",            0.92, 0.20, 0.06, 0.99, 0.09, -19.2, 0.04, 48,  0.12),
    ("INJ059", "Comptine d'un autre ete",  "Yann Tiersen",         0.98, 0.35, 0.19, 0.97, 0.09, -14.8, 0.04, 76,  0.42),
    ("INJ060", "The Ludlows",              "James Horner",         0.97, 0.23, 0.14, 0.96, 0.10, -15.6, 0.04, 72,  0.28),

    # LIVE SOUNDING / HIGH LIVENESS
    ("INJ061", "Bohemian Rhapsody Live",   "Queen",                0.34, 0.37, 0.75, 0.00, 0.83, -5.4, 0.09, 72,  0.52),
    ("INJ062", "Black Dog Live",           "Led Zeppelin",         0.05, 0.52, 0.88, 0.00, 0.79, -4.2, 0.06, 103, 0.42),
    ("INJ063", "Wish You Were Here Live",  "Pink Floyd",           0.71, 0.38, 0.52, 0.02, 0.81, -8.1, 0.05, 65,  0.46),
    ("INJ064", "Brown Eyed Girl Live",     "Van Morrison",         0.15, 0.68, 0.72, 0.00, 0.80, -5.8, 0.07, 148, 0.86),
    ("INJ065", "Ring of Fire Live",        "Johnny Cash",          0.72, 0.55, 0.64, 0.00, 0.77, -7.4, 0.06, 119, 0.78),
    ("INJ066", "Africa Live",              "Weezer",               0.06, 0.58, 0.79, 0.00, 0.82, -5.1, 0.05, 93,  0.84),

    # SPEECH HEAVY / HIP-HOP
    ("INJ067", "HUMBLE.",                  "Kendrick Lamar",       0.00, 0.54, 0.57, 0.00, 0.10, -6.8, 0.30, 150, 0.42),
    ("INJ068", "DNA.",                     "Kendrick Lamar",       0.00, 0.47, 0.74, 0.00, 0.12, -5.1, 0.38, 141, 0.39),
    ("INJ069", "SICKO MODE",               "Travis Scott",         0.00, 0.53, 0.73, 0.00, 0.09, -4.8, 0.27, 155, 0.37),
    ("INJ070", "Rap God",                  "Eminem",               0.00, 0.48, 0.80, 0.00, 0.11, -4.0, 0.45, 148, 0.35),
    ("INJ071", "Numb/Encore",              "Jay-Z & Linkin Park",  0.00, 0.62, 0.85, 0.00, 0.14, -3.9, 0.22, 120, 0.38),
    ("INJ072", "Gold Digger",              "Kanye West",           0.01, 0.74, 0.79, 0.00, 0.11, -4.7, 0.21, 110, 0.69),
    ("INJ073", "Empire State of Mind",     "Jay-Z",                0.02, 0.66, 0.70, 0.00, 0.12, -5.3, 0.25, 91,  0.73),
    ("INJ074", "Forgot About Dre",         "Dr. Dre",              0.00, 0.60, 0.74, 0.00, 0.10, -5.2, 0.32, 96,  0.44),

    # R&B / SOUL / MOTOWN
    ("INJ075", "Superstition",             "Stevie Wonder",        0.03, 0.79, 0.80, 0.00, 0.09, -5.2, 0.05, 100, 0.82),
    ("INJ076", "I Heard It Through Grapevine","Marvin Gaye",       0.05, 0.70, 0.62, 0.00, 0.11, -7.1, 0.05, 113, 0.74),
    ("INJ077", "Ain't No Mountain High",   "Marvin Gaye",          0.04, 0.72, 0.65, 0.00, 0.10, -7.5, 0.06, 107, 0.89),
    ("INJ078", "Respect",                  "Aretha Franklin",      0.02, 0.73, 0.82, 0.00, 0.11, -5.0, 0.07, 114, 0.95),
    ("INJ079", "Dancing in the Street",    "Martha Reeves",        0.03, 0.78, 0.74, 0.00, 0.09, -6.2, 0.05, 139, 0.96),
    ("INJ080", "My Girl",                  "The Temptations",      0.10, 0.67, 0.56, 0.00, 0.10, -8.4, 0.05, 105, 0.95),

    # INDIE / ALT-POP
    ("INJ081", "Mr. Brightside",           "The Killers",          0.00, 0.34, 0.93, 0.00, 0.11, -4.2, 0.05, 148, 0.28),
    ("INJ082", "Tongue Tied",              "Grouplove",            0.01, 0.54, 0.82, 0.00, 0.12, -4.6, 0.06, 148, 0.73),
    ("INJ083", "Young Volcanoes",          "Fall Out Boy",         0.01, 0.48, 0.71, 0.00, 0.10, -5.7, 0.06, 133, 0.69),
    ("INJ084", "Polaroid",                 "Imagine Dragons",      0.02, 0.58, 0.68, 0.00, 0.11, -5.5, 0.05, 100, 0.61),
    ("INJ085", "Ribs",                     "Lorde",                0.10, 0.45, 0.48, 0.00, 0.10, -7.8, 0.04, 130, 0.36),
    ("INJ086", "Green Light",              "Lorde",                0.01, 0.64, 0.74, 0.00, 0.09, -4.8, 0.04, 130, 0.56),
    ("INJ087", "505",                      "Arctic Monkeys",       0.19, 0.32, 0.53, 0.00, 0.10, -7.1, 0.04, 118, 0.32),
    ("INJ088", "R U Mine?",                "Arctic Monkeys",       0.00, 0.41, 0.87, 0.00, 0.12, -4.3, 0.05, 100, 0.44),

    # JAZZ / BLUES
    ("INJ089", "Take Five",                "Dave Brubeck",         0.91, 0.52, 0.27, 0.96, 0.12, -12.3, 0.04, 172, 0.60),
    ("INJ090", "So What",                  "Miles Davis",          0.88, 0.40, 0.22, 0.97, 0.10, -14.1, 0.04, 136, 0.40),
    ("INJ091", "Round Midnight",           "Thelonious Monk",      0.93, 0.33, 0.18, 0.98, 0.11, -15.2, 0.04, 68,  0.22),
    ("INJ092", "Autumn Leaves",            "Bill Evans",           0.95, 0.38, 0.15, 0.98, 0.10, -15.8, 0.04, 82,  0.31),
    ("INJ093", "Crossroads",               "Robert Johnson",       0.72, 0.56, 0.62, 0.02, 0.18, -8.7, 0.06, 120, 0.54),
    ("INJ094", "The Thrill Is Gone",       "B.B. King",            0.68, 0.48, 0.44, 0.03, 0.14, -9.6, 0.05, 68,  0.24),

    # LATIN / REGGAETON
    ("INJ095", "Despacito",                "Luis Fonsi",           0.22, 0.69, 0.68, 0.00, 0.12, -5.3, 0.07, 89,  0.83),
    ("INJ096", "Con Calma",                "Daddy Yankee",         0.03, 0.84, 0.82, 0.00, 0.09, -3.8, 0.07, 96,  0.87),
    ("INJ097", "Bailando",                 "Enrique Iglesias",     0.06, 0.80, 0.74, 0.00, 0.10, -4.9, 0.06, 130, 0.90),
    ("INJ098", "La Bamba",                 "Ritchie Valens",       0.18, 0.76, 0.79, 0.00, 0.14, -5.8, 0.06, 177, 0.94),

    # VERY SLOW / BALLADS
    ("INJ099", "Hello",                    "Adele",                0.42, 0.42, 0.30, 0.00, 0.11, -8.0, 0.03, 79,  0.22),
    ("INJ100", "All of Me",                "John Legend",          0.50, 0.35, 0.31, 0.00, 0.09, -8.7, 0.03, 63,  0.54),
    ("INJ101", "A Thousand Years",         "Christina Perri",      0.76, 0.34, 0.29, 0.00, 0.09, -9.7, 0.03, 95,  0.42),
    ("INJ102", "Thinking Out Loud",        "Ed Sheeran",           0.42, 0.50, 0.43, 0.00, 0.10, -7.9, 0.04, 79,  0.69),
    ("INJ103", "Perfect",                  "Ed Sheeran",           0.64, 0.37, 0.45, 0.00, 0.10, -8.2, 0.03, 95,  0.70),
    ("INJ104", "Make You Feel My Love",    "Adele",                0.84, 0.28, 0.22, 0.00, 0.09, -11.5, 0.03, 69,  0.48),

    # VERY HIGH TEMPO (180+ BPM)
    ("INJ105", "Shake Your Body",          "Michael Jackson",      0.04, 0.82, 0.75, 0.00, 0.10, -5.5, 0.05, 187, 0.94),
    ("INJ106", "Jump Around",              "House of Pain",        0.00, 0.64, 0.87, 0.00, 0.12, -3.9, 0.16, 185, 0.67),
    ("INJ107", "Sabotage",                 "Beastie Boys",         0.00, 0.52, 0.93, 0.00, 0.13, -3.4, 0.14, 186, 0.55),
    ("INJ108", "Firestarter",              "The Prodigy",          0.00, 0.59, 0.97, 0.00, 0.11, -2.8, 0.11, 166, 0.48),
    ("INJ109", "Breathe",                  "The Prodigy",          0.00, 0.62, 0.95, 0.06, 0.10, -3.2, 0.09, 164, 0.44),

    # AMBIENT / VERY LOW ENERGY
    ("INJ110", "Weightless",               "Marconi Union",        0.94, 0.19, 0.04, 0.99, 0.09, -21.3, 0.04, 60,  0.10),
    ("INJ111", "Spiegel im Spiegel",       "Arvo Part",            0.99, 0.16, 0.05, 0.99, 0.09, -22.1, 0.04, 54,  0.18),
    ("INJ112", "Stars",                    "Moby",                 0.88, 0.22, 0.08, 0.97, 0.09, -18.6, 0.04, 72,  0.28),

    # POP CLASSICS
    ("INJ113", "Billie Jean",              "Michael Jackson",      0.00, 0.77, 0.72, 0.01, 0.10, -4.9, 0.06, 117, 0.67),
    ("INJ114", "Material Girl",            "Madonna",              0.03, 0.80, 0.79, 0.00, 0.09, -5.2, 0.06, 131, 0.85),
    ("INJ115", "Like a Prayer",            "Madonna",              0.12, 0.65, 0.74, 0.00, 0.11, -5.6, 0.04, 116, 0.69),
    ("INJ116", "Girls Just Wanna Have Fun","Cyndi Lauper",         0.04, 0.69, 0.77, 0.00, 0.10, -5.5, 0.05, 120, 0.93),
    ("INJ117", "Don't Stop Believin",      "Journey",              0.01, 0.56, 0.85, 0.00, 0.11, -4.4, 0.05, 119, 0.78),
    ("INJ118", "Sweet Caroline",           "Neil Diamond",         0.17, 0.56, 0.59, 0.00, 0.15, -7.6, 0.06, 126, 0.90),
    ("INJ119", "Piano Man",                "Billy Joel",           0.69, 0.57, 0.44, 0.02, 0.11, -9.0, 0.06, 168, 0.75),
    ("INJ120", "September",                "Earth Wind and Fire",  0.02, 0.82, 0.82, 0.00, 0.10, -4.8, 0.06, 126, 0.97),
]


def main():
    engine = get_engine()

    # get all real (non-fake) users so every real Spotify user gets the injected catalog
    with engine.connect() as conn: # type: ignore
        user_rows = conn.execute(
            text("SELECT user_id, spotify_user_hash FROM users WHERE spotify_user_hash NOT LIKE 'fake_%' ORDER BY user_id")
        ).fetchall()
        if not user_rows:
            print("❌ No real users in the database. Log in through Streamlit first.")
            return
        print(f"Found {len(user_rows)} real user(s): {[r[1] for r in user_rows]}")

    tracks_inserted = 0
    features_inserted = 0
    catalog_inserted = 0

    with engine.begin() as conn:
        for song in SONGS:
            (tid, name, artist,
             acousticness, danceability, energy, instrumentalness,
             liveness, loudness, speechiness, tempo, valence) = song

            # insert track
            conn.execute(text("""
                INSERT INTO tracks (spotify_track_id, name, primary_artist, popularity, duration_ms, explicit, spotify_url)
                VALUES (:id, :name, :artist, :pop, :dur, false, :url)
                ON CONFLICT (spotify_track_id) DO NOTHING
            """), {
                "id": tid, "name": name, "artist": artist,
                "pop": 50, "dur": int(tempo * 1000),
                "url": f"https://open.spotify.com/track/{tid}",
            })
            tracks_inserted += 1

            # insert audio features
            conn.execute(text("""
                INSERT INTO audio_features (
                    spotify_track_id, acousticness, danceability, energy,
                    instrumentalness, liveness, loudness, speechiness, tempo, valence,
                    key, mode, source
                )
                VALUES (
                    :id, :ac, :da, :en, :ins, :li, :lo, :sp, :te, :va,
                    0, 1, 'injected'
                )
                ON CONFLICT (spotify_track_id) DO NOTHING
            """), {
                "id": tid, "ac": acousticness, "da": danceability, "en": energy,
                "ins": instrumentalness, "li": liveness, "lo": loudness,
                "sp": speechiness, "te": tempo, "va": valence,
            })
            features_inserted += 1

            # insert catalog entry for every real user
            for user_id, _ in user_rows:
                conn.execute(text("""
                    INSERT INTO catalog_tracks (user_id, spotify_track_id, source, query_used, seed_type, seed_value)
                    VALUES (:uid, :tid, 'injected', 'manual_injection', 'injected', 'diverse_test_set')
                    ON CONFLICT (user_id, spotify_track_id, seed_type, seed_value) DO NOTHING
                """), {"uid": user_id, "tid": tid})
                catalog_inserted += 1

    print(f"✅ Tracks inserted:        {tracks_inserted}")
    print(f"✅ Audio features inserted: {features_inserted}")
    print(f"✅ Catalog entries added:   {catalog_inserted}")
    print(f"\nTotal songs in injection: {len(SONGS)}")
    print("Run the pipeline again to cluster and rank the expanded catalog.")


if __name__ == "__main__":
    main()