import json
from datetime import datetime

import googleapiclient.discovery
import googleapiclient.errors

from youtube_transcript_api import YouTubeTranscriptApi

apikeys = json.load(open("../apikeys/keys.json","r+"))

def video_meta_data(vid,part="snippet,contentDetails"):
	# meta data from youtube api, returns json 
	api_service_name = "youtube"
	api_version = "v3"

	youtube = googleapiclient.discovery.build(
	    api_service_name, api_version, developerKey=apikeys['youtube'])

	request = youtube.videos().list(
	    part=part,
	    id=vid
	)
	response = request.execute()

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

def get_content(vid):
    final = {}
    meta = video_meta_data(vid)

    if (not is_meta_data_valid(meta)):
        return final

    snippet = meta['items'][0]['snippet']
    contentDetails = meta['items'][0]['contentDetails']

    final['vid'] = vid
    final['title'] = snippet['title']
    final['date'] = datetime.strptime(snippet['publishedAt'],'%Y-%m-%dT%H:%M:%SZ')
    final['categoryId'] = snippet['categoryId']
    final['description'] = snippet['description']
    final['channelId'] = snippet['channelId']
    final['hasCaptions'] = contentDetails['caption']
    final['duration'] = contentDetails['duration']
    final['captions'] = get_captions(vid) if contentDetails['caption'] =='true' else []

    return final

def has_captions(vid):
    meta = video_meta_data(vid)
    if (not is_meta_data_valid(meta)):
        return False
        
    return meta['items'][0]['contentDetails']['caption'] == 'true'