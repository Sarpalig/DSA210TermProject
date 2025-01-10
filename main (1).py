import json
import pandas as pd
from googleapiclient.discovery import build
import os
import re
import time
import isodate  # To parse ISO 8601 duration format

# Ensure the 'data' directory exists
os.makedirs("data", exist_ok=True)

# Step 1: Load the JSON data
input_file = "watch-history.json"
if not os.path.exists(input_file):
    print(f"Error: '{input_file}' not found. Please ensure the file exists.")
    exit()

with open(input_file, "r", encoding="utf-8") as file:
    data = json.load(file)

# Step 2: Extract relevant fields and organize the data
cleaned_data = []
for entry in data:
    title = entry.get("title", "Unknown")
    video_url = entry.get("titleUrl", None)
    channel_name = entry.get("subtitles", [{}])[0].get("name", "Unknown")
    
    cleaned_data.append({
        "title": title,
        "time": entry.get("time", None),
        "video_url": video_url,
        "channel": channel_name
    })

# Step 3: Create a DataFrame
df = pd.DataFrame(cleaned_data)

# Convert 'time' to datetime format
df["time"] = pd.to_datetime(df["time"], errors="coerce")

# Step 4: Extract Video IDs from URLs
def extract_video_id(url):
    if pd.isna(url):
        return None
    match = re.search(r"(?:v=|\/)([0-9A-Za-z_-]{11}).*", url)
    return match.group(1) if match else None

df["video_id"] = df["video_url"].apply(extract_video_id)

# Step 5: Set up YouTube Data API
API_KEY = "AIzaSyBLN4rvHXGNENWgLeW2F0yC-Qzv8LBBXa4"  
youtube = build("youtube", "v3", developerKey=API_KEY)

# Step 6: Fetch Video Categories, Titles, and Durations from the API and Save Progress
def fetch_video_data(video_ids, batch_size=50):
    progress_file = "data/fetch_progress.csv"
    fetched_data = []

    # Load existing progress if the file exists
    if os.path.exists(progress_file):
        existing_progress = pd.read_csv(progress_file)
        fetched_data = existing_progress.to_dict("records")
        fetched_video_ids = set(existing_progress["video_id"])
        print(f"Loaded progress for {len(fetched_video_ids)} videos.")
    else:
        fetched_video_ids = set()

    try:
        for i in range(0, len(video_ids), batch_size):
            batch_ids = [vid for vid in video_ids[i:i+batch_size] if vid not in fetched_video_ids]
            if not batch_ids:
                continue

            print(f"Fetching batch {i//batch_size + 1}: {batch_ids}")
            response = youtube.videos().list(part="snippet,contentDetails", id=",".join(batch_ids)).execute()
            
            for item in response.get("items", []):
                video_id = item["id"]
                title = item["snippet"].get("title", "Unknown")
                category_id = item["snippet"].get("categoryId", "Unknown")
                channel_title = item["snippet"].get("channelTitle", "Unknown")
                duration = item["contentDetails"].get("duration", "PT0S")

                # Parse ISO 8601 duration format to seconds
                parsed_duration = isodate.parse_duration(duration).total_seconds()

                fetched_data.append({
                    "video_id": video_id,
                    "title": title,
                    "category_id": category_id,
                    "channel": channel_title,
                    "duration_seconds": parsed_duration
                })

                # Save progress after each video
                pd.DataFrame(fetched_data).to_csv(progress_file, index=False)

            time.sleep(1)  # Add a delay to avoid hitting rate limits
    except Exception as e:
        print(f"Error fetching data: {e}")

    return fetched_data

# Step 7: Run the Fetching Process
unique_video_ids = df["video_id"].dropna().unique().tolist()
fetched_data = fetch_video_data(unique_video_ids)

# Step 8: Map Fetched Data to Original DataFrame
fetched_df = pd.DataFrame(fetched_data)
merged_df = df.merge(fetched_df, on="video_id", how="left", suffixes=("_original", ""))

# Step 9: Save the Final DataFrame with Categories and Durations
output_path = "data/with_categories_and_durations.csv"
merged_df.to_csv(output_path, index=False)

print(f"Fetching complete. Data saved to '{output_path}'.")
