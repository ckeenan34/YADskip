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

playlistBatchSize = 10

def toDate(date):
    return datetime.strptime(date[:19],'%Y-%m-%dT%H:%M:%S')

def video_meta_data(vids,part="snippet,contentDetails"):
    # logging.info('called yt.video_meta_data')

    final = None
    split_vids = vids.split(",")
    
    video_limit = 50
    for x in range(0,len(split_vids), video_limit):
        logging.info("on video #{} of #{}".format(x,len(split_vids)))
        new_vids = ",".join(split_vids[x:x+video_limit])
        # meta data from youtube api, returns json 
        request = youtube.videos().list(
            part=part,
            id=new_vids
        )
        response = request.execute()

        if (final):
            final["items"] += response["items"]
        else:
            final = response

    return final

def channel_meta_data(cid,part="snippet,contentDetails,statistics"):
    logging.info('called yt.channel_meta_data')
    request = youtube.channels().list(
        part=part,
        id=cid
    )
    response = request.execute()

    return response

#I'm tired I can name this however I want
def multiple_channel_meta_datas(cids,part="snippet,contentDetails,statistics"):
    logging.info('called yt.multiple_channel_meta_datas')

    final = None
    split_cids = cids.split(",")
    
    channel_limit = 50
    for x in range(0,len(split_cids), channel_limit):
        logging.info("on video #{} of #{}".format(x,len(split_cids)))
        new_cids = ",".join(split_cids[x:x+channel_limit])
        # meta data from youtube api, returns json 
        request = youtube.channels().list(
            part=part,
            id=new_cids
        )
        response = request.execute()

        if (final):
            final["items"] += response["items"]
        else:
            final = response

    return final

def playlistitems_meta_data(pid, part="snippet", stopdate=datetime(1970,1,1), maxResults = 10):
    logging.info('called yt.playlistitems_meta_data')
    nextToken = None
    previous_items = []
    while True:
        logging.info("total playlist items so far: #{}".format(len(previous_items)))
        request = youtube.playlistItems().list(
            part=part,
            maxResults=maxResults,
            pageToken= nextToken,
            playlistId=pid
        )
        response = request.execute()

        oldest_vid = toDate(response['items'][-1]['snippet']['publishedAt'])
        if "nextPageToken" in response and oldest_vid > stopdate:
            nextToken = response["nextPageToken"]
            previous_items += response['items']
        else:
            break

    response['items']=previous_items + response['items']
    return response

def is_meta_data_valid(meta):
    return 'items' in meta and isinstance(meta['items'],list) and len(meta['items']) > 0

def get_captions(vid):
    try:
        res= YouTubeTranscriptApi.get_transcript(vid)
    except:
        print("Getting captions failed for {}, likely captions don't exists".format(vid))
        return None

    return res if res != [] else None

def get_video_metas(vids, include_captions = False):
    # logging.info('called yt.get_video_metas')
    final = []
    meta = video_meta_data(vids)

    if (not is_meta_data_valid(meta)):
        return final

    leftover = set(vids.split(","))

    for item in meta['items']:
        video = {}

        vid = item['id']
        snippet = item['snippet']
        contentDetails = item['contentDetails']

        if vid in leftover:
            leftover.remove(vid)

        video['vid'] = vid
        video['title'] = snippet['title']
        video['publishedAt'] = toDate(snippet['publishedAt'])
        video['categoryId'] = snippet['categoryId']
        video['description'] = snippet['description']
        video['channelId'] = snippet['channelId']
        video['hasCaptions'] = contentDetails['caption']
        video['duration'] = contentDetails['duration']
        video['status'] = True
        if (include_captions):
            video['captions'] = get_captions(vid) if contentDetails['caption'] =='true' else None
            video['captionAttemps'] = 1

        final.append(video)
    
    for vid in leftover:
        video = {}
        video['vid'] = vid
        video['captions'] = None
        video['captionAttemps'] = 100
        video['status'] = False
        video['statusMessage'] = "No Data from youtube API"
        final.append(video)

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
    final['publishedAt'] = toDate(snippet['publishedAt'])
    final['catalogId'] = contentDetails['relatedPlaylists']['uploads']
    final['videoCount'] = statistics['videoCount']

    return final

def get_channel_metas(cids):
    final = []
    meta = multiple_channel_meta_datas(cids)
    if (not is_meta_data_valid(meta)):
        return final

    for item in meta["items"]:
        channel = {}
        snippet = item['snippet']
        contentDetails = item['contentDetails']
        statistics = item['statistics']

        channel['cid'] = item['id']
        channel['name'] = snippet['title']
        channel['publishedAt'] = toDate(snippet['publishedAt'])
        channel['catalogId'] = contentDetails['relatedPlaylists']['uploads']
        channel['videoCount'] = statistics['videoCount']

        final.append(channel)
    
    return final

def get_playlistitem_meta(pid, stopdate=datetime(1970,1,1)):
    logging.info('called yt.get_playlistitem_meta')
    final = {}

    meta = playlistitems_meta_data(pid,stopdate=stopdate,maxResults=playlistBatchSize)
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

        final['videos'].append(video)

    return final

# def has_captions(vid):
#     logging.info('called yt.has_captions')
#     meta = video_meta_data(vid)
#     if (not is_meta_data_valid(meta)):
#         return False
        
#     return meta['items'][0]['contentDetails']['caption'] == 'true'


# def get_playlistitem_ids(pid, stopdate=datetime(1970,1,1)):
#     logging.info('called yt.get_playlistitem_ids')
#     final = {}

#     meta = playlistitems_meta_data(pid,stopdate=stopdate)
#     if (not is_meta_data_valid(meta)):
#         return final

#     final['pid'] = pid
#     final['vids'] = []

#     for item in meta['items']:
#         snippet = item['snippet']
#         if (toDate(snippet['publishedAt']) > stopdate):
#             final['vids'].append(snippet['resourceId']['videoId'])

#     return final