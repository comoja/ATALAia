import json
from youtube_transcript_api import YouTubeTranscriptApi

videos = ["OtwhMNYzCyo", "-GyFAL2j6W8", "idZ9n1dUZUQ"]

for v in videos:
    print(f"\n--- TRANSCRIPT FOR {v} ---")
    try:
        transcript = YouTubeTranscriptApi.get_transcript(v, languages=['es', 'en'])
        text = " ".join([t['text'] for t in transcript])
        # Just print first 500 characters and last 500 characters to save tokens, wait... 
        # Actually I need the full trading concept. Let's print the whole thing, YouTube shorts are short, the long one might be 20 mins.
        # But wait, YouTubeTranscriptApi might throw exception if subtitles aren't available.
        # Let's see.
        text = text.replace('\n', ' ')
        print(text[:3000] + "\n...\n" + text[-3000:] if len(text) > 6000 else text)
    except Exception as e:
        print(f"Error: {e}")
