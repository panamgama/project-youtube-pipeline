CREATE OR REPLACE VIEW "all_data_all_dates" AS 
SELECT
  channelname channel_name
, subscribercount subscriber_count
, concat('https://www.youtube.com/channel/', channel_data.channelid) channel_url
, channeltier channel_tier
, title
, likecount like_count
, video_data.viewcount view_count
, commentcount comment_count
, publishedat_utc_date published_date
, dense_rank() OVER (PARTITION BY video_data.channelid ORDER BY publishedat_utc_date DESC, videoid ASC) vid_publish_rank
, duration_seconds_total duration_seconds
, concat('https://www.youtube.com/watch?v=', videoid) video_url
, date_add('day', 1, video_data.extractdate) extract_date
FROM
  video_data
INNER JOIN 
  channel_data 
ON 
  ((channel_data.channelid = video_data.channelid) 
  AND 
  (channel_data.extractdate = video_data.extractdate))
