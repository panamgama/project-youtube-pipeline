def get_all_video_ids(youtube, playlist_id):
    """
    Get a list of IDs of all videos in a given YouTube playlist.

    Params:
        youtube: The build object from googleapiclient.discovery
        playlist_id: ID of a YouTube playlist

    Returns:
        List of video IDs of all videos in the playlist.
    """
    video_ids = []
    next_page_token = None

    while True:
        request = youtube.playlistItems().list(
            part = "contentDetails",
            playlistId = playlist_id,
            maxResults = 50, # API allows max 50 video IDs per request
            pageToken = next_page_token
        )
        response = request.execute()

        # Using list comprehension to extract video IDs
        video_ids.extend(item["contentDetails"]["videoId"] for item in response.get("items", []))
        
        next_page_token = response.get("nextPageToken")
        if not next_page_token:
            break

    return video_ids
