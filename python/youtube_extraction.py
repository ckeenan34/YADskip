import json
from datetime import datetime, timedelta

import googleapiclient.discovery
import googleapiclient.errors

from youtube_transcript_api import YouTubeTranscriptApi

from log import logging

apikeys = json.load(open("../apikeys/keys.json","r+"))
api_service_name = "youtube"
api_version = "v3"
youtube = googleapiclient.discovery.build(
    api_service_name, api_version, developerKey=apikeys['youtube'])

def toDate(date):
    return datetime.strptime(date,'%Y-%m-%dT%H:%M:%SZ')

def video_meta_data(vids,part="snippet,contentDetails"):
    logging.info('called yt.video_meta_data')
    # meta data from youtube api, returns json 
    request = youtube.videos().list(
        part=part,
        id=vids
    )
    response = request.execute()

    return response

def channel_meta_data(cid,part="snippet,contentDetails,statistics"):
    logging.info('called yt.channel_meta_data')
    request = youtube.channels().list(
        part=part,
        id=cid
    )
    response = request.execute()

    return response

def playlistitems_meta_data(pid, part="snippet"):
    logging.info('called yt.playlistitems_meta_data')
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
    logging.info('called yt.is_meta_data_valid')
    return 'items' in meta and isinstance(meta['items'],list) and len(meta['items']) > 0

def get_captions(vid):
    logging.info('called yt.get_captions')
    try:
        res= YouTubeTranscriptApi.get_transcript(vid)
    except:
        print("Getting captions failed for {}, likely captions don't exists".format(vid))
        return []

    return res

def get_video_metas(vids):
    logging.info('called yt.get_video_metas')
    final = []
    meta = video_meta_data(vids)

    if (not is_meta_data_valid(meta)):
        return final

    for item in meta['items']:
        video = {}

        vid = item['id']
        snippet = item['snippet']
        contentDetails = item['contentDetails']

        video['vid'] = vid
        video['title'] = snippet['title']
        video['publishedAt'] = toDate(snippet['publishedAt'])
        video['categoryId'] = snippet['categoryId']
        video['description'] = snippet['description']
        video['channelId'] = snippet['channelId']
        video['hasCaptions'] = contentDetails['caption']
        video['duration'] = contentDetails['duration']
        video['captions'] = get_captions(vid) if contentDetails['caption'] =='true' else []
        video['captionAttemps'] = 1

        final.append(video)

    return final

def get_channel_meta(cid):
    logging.info('called yt.get_channel_meta')
    final = {}
    meta = channel_meta_data(cid)

    if (not is_meta_data_valid(meta)):
        return final
    
    snippet = meta['items'][0]['snippet']
    contentDetails = meta['items'][0]['contentDetails']
    statistics = meta['items'][0]['statistics']

    final['cid'] = cid
    final['name'] = snippet['title']
    final['publishedAt'] = toDate(snippet['publishedAt'])
    final['catalogId'] = contentDetails['relatedPlaylists']['uploads']
    final['videoCount'] = statistics['videoCount']

    return final

def get_playlistitem_ids(pid, sdate=datetime(1970,1,1)):
    logging.info('called yt.get_playlistitem_ids')
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

def get_playlistitem_meta(pid):
    logging.info('called yt.get_playlistitem_meta')
    final = {}

    meta = playlistitems_meta_data(pid)
    if (not is_meta_data_valid(meta)):
        return final

    final['pid'] = pid
    final['videos'] = []

    for item in meta['items']:
        video = {}
        snippet = item['snippet']

        video['vid'] = snippet['resourceId']['videoId']
        video['title'] = snippet['title']
        video['publishedAt'] = toDate(snippet['publishedAt'])
        video['description'] = snippet['description']
        video['channelId'] = snippet['channelId']
        video['captionAttemps'] = 0

        final['vids'].append(video)

    return final

def has_captions(vid):
    logging.info('called yt.has_captions')
    meta = video_meta_data(vid)
    if (not is_meta_data_valid(meta)):
        return False
        
    return meta['items'][0]['contentDetails']['caption'] == 'true'