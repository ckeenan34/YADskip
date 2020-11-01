import json
from datetime import datetime, timedelta

import googleapiclient.discovery
import googleapiclient.errors

from youtube_transcript_api import YouTubeTranscriptApi

apikeys = json.load(open("../apikeys/keys.json","r+"))
api_service_name = "youtube"
api_version = "v3"
youtube = googleapiclient.discovery.build(
    api_service_name, api_version, developerKey=apikeys['youtube'])

def toDate(date):
    return datetime.strptime(date,'%Y-%m-%dT%H:%M:%SZ')

def video_meta_data(vid,part="snippet,contentDetails"):
	# meta data from youtube api, returns json 
	request = youtube.videos().list(
	    part=part,
	    id=vid
	)
	response = request.execute()

	return response

def channel_meta_data(cid,part="snippet,contentDetails,statistics"):
    request = youtube.channels().list(
        part=part,
        id=cid
    )
    response = request.execute()

    return response

def playlistitems_meta_data(pid, part="snippet"):
    nextToken = None
    previous_items = []
    while True:
        request = youtube.playlistItems().list(
            part=part,
            maxResults=50,
            pageToken= nextToken,
            playlistId=pid
        )
        response = request.execute()

        if "nextPageToken" in response:
            nextToken = response["nextPageToken"]
            previous_items += response['items']
        else:
            break

    response['items']+=previous_items
    return response

def is_meta_data_valid(meta):
    return 'items' in meta and isinstance(meta['items'],list) and len(meta['items']) > 0

def get_captions(vid):
    try:
        res= YouTubeTranscriptApi.get_transcript(vid)
    except:
        print("Getting captions failed for {}, likely captions don't exists".format(vid))
        return []

    return res

def get_video_meta(vid):
    final = {}
    meta = video_meta_data(vid)

    if (not is_meta_data_valid(meta)):
        return final

    snippet = meta['items'][0]['snippet']
    contentDetails = meta['items'][0]['contentDetails']

    final['vid'] = vid
    final['title'] = snippet['title']
    final['date'] = toDate(snippet['publishedAt'])
    final['categoryId'] = snippet['categoryId']
    final['description'] = snippet['description']
    final['channelId'] = snippet['channelId']
    final['hasCaptions'] = contentDetails['caption']
    final['duration'] = contentDetails['duration']
    final['captions'] = get_captions(vid) if contentDetails['caption'] =='true' else []

    return final

def get_channel_meta(cid):
    final = {}
    meta = channel_meta_data(cid)

    if (not is_meta_data_valid(meta)):
        return final
    
    snippet = meta['items'][0]['snippet']
    contentDetails = meta['items'][0]['contentDetails']
    statistics = meta['items'][0]['statistics']

    final['cid'] = cid
    final['name'] = snippet['title']
    final['created'] = toDate(snippet['publishedAt'])
    final['catalogId'] = contentDetails['relatedPlaylists']['uploads']
    final['videoCount'] = statistics['videoCount']

    return final

def get_playlistitem_meta(pid, sdate=datetime(1970,1,1,1,1)):
    final = {}

    meta = playlistitems_meta_data(pid)
    if (not is_meta_data_valid(meta)):
        return final

    final['pid'] = pid
    final['vids'] = []

    for item in meta['items']:
        snippet = item['snippet']
        if (toDate(snippet['publishedAt']) > sdate):
            final['vids'].append(snippet['resourceId']['videoId'])

    return final

def has_captions(vid):
    meta = video_meta_data(vid)
    if (not is_meta_data_valid(meta)):
        return False
        
    return meta['items'][0]['contentDetails']['caption'] == 'true'