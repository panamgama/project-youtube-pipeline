from datetime import datetime

def get_channel_details(youtube_obj, channel_ids):
    """
    Get statistics for a specified list of YouTube channels.

    Params:
        youtube_obj: The build object from googleapiclient.discovery
        channel_ids: List of channel IDs to query

    Returns:
        List of dictionaries containing channel statistics: 
        id, name, subscriber count, view count, video count, upload playlist ID
    """
    request = youtube_obj.channels().list(
        part="snippet,contentDetails,statistics",
        id=",".join(channel_ids)
    )
    response = request.execute()

    # Structuring data within list
    all_data = [
        {
            "channelId": item["id"],
            "channelName": item["snippet"]["title"],
            "subscriberCount": item["statistics"].get("subscriberCount", 0),
            "videoCount": item["statistics"].get("videoCount", 0),
            "viewCount": item["statistics"].get("viewCount", 0),
            "uploadPlaylistId": item["contentDetails"]["relatedPlaylists"]["uploads"],
            'extractDate': datetime.today().date()
        }
        for item in response.get("items", [])
    ]

    return all_data