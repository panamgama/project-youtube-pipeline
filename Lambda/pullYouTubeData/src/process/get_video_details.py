from datetime import datetime

def get_video_details(youtube, video_ids):
    """
    Get statistics for a list of given YouTube video IDs.

    Params:
        youtube: The build object from googleapiclient.discovery
        video_ids: List of video IDs

    Returns:
        List of dictionaries containing video statistics:
            'channelTitle', 'title', 'description', 'tags', 'publishedAt',
            'viewCount', 'likeCount', 'favoriteCount', 'commentCount',
            'duration', 'definition', 'caption'.
    """
    
    stats_to_keep = {
        "snippet": [
            "title", 
            "description", 
            "tags", 
            "publishedAt"
            ],
        "statistics": [
            "viewCount", 
            "likeCount", 
            "favoriteCount", 
            "commentCount"
            ],
        "contentDetails": [
            "duration", 
            "definition", 
            "caption"
            ]
    }
    
    all_video_info = []

    for i in range(0, len(video_ids), 50):  # API allows max 50 videos per request
        request = youtube.videos().list(
            part="snippet,contentDetails,statistics",
            id=",".join(video_ids[i:i+50])
        )
        response = request.execute()

        # Unpack to all_video_info list
        all_video_info.extend(
            {
                "videoId": video["id"],
                **{
                    v: video.get(k, {}).get(v, None)
                    for k, values in stats_to_keep.items()
                    for v in values
                },
                'extractDate': datetime.today().date()
            }
            for video in response.get("items", [])
        )

    return all_video_info
