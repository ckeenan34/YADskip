from pymongo import MongoClient
from bson.objectid import ObjectId

import youtube_extraction as yt 
from log import logging

from datetime import datetime, timedelta

client = MongoClient('mongodb://admin:<password here>@192.168.0.18:27017')
db=client.admin

channels = db.channels
videos = db.videos

# removed videos that are in the collection and already have captions or failed to get captions too many times
def filter_vids(vids):
    logging.info("called mf.filter_vids")
    filtered_vids = []
    for vid in vids:
        video = videos.find_one({'vid':vid})
        if (not video or (not video['captions'] and video['captionAttemps']<=3)):
            filtered_vids.append(vid)

    logging.info("Filtered #{} to #{} of videos".format(len(vids),len(filtered_vids)))
    return filtered_vids

def add_channel(cid):
    logging.info("called mf.add_channel")
    if (channels.find_one({'cid': cid})):
        print("channel already exists")
        return False

    cmeta = yt.get_channel_meta(cid)

    cmeta['dateCutoff'] = datetime(1970,1,1)
    cmeta['checkAgain'] = datetime.now() + timedelta(hours=23)

    result = channels.insert_one(cmeta)

    return result.acknowledged

def add_videos(vids):
    logging.info("called mf.add_videos")
    vids = filter_vids(vids)

    vmetas = yt.get_video_metas(','.join(vids))

    if (not vmetas):
        return []

    for vmeta in vmetas:
        video = videos.find_one({'vid':vmeta['vid']})
        if video:
            vmeta['captionAttemps'] += video['captionAttemps']

    return videos.insert_many(vmetas).inserted_ids

# def add_all_video_meta(pid):
#     logging.info("called yt.add_all_video_meta")
#     vmeta = yt.get_playlistitem_meta(pid)

#     videos.


def starter_channels():
    logging.info("called mf.starter_channels")
    cids = ["UCKzJFdi57J53Vr_BkTfN3uQ","UCeeFfhMcJa1kjtfZAGskOCA","UCXuqSBlHAE6Xw-yeJA0Tunw","UC6107grRI4m0o2-emgoDnAA","UC0QHWhjbe5fGJEPz3sVb6nw","UCyWDmyZRjrGHeKF-ofFsT5Q","UCravYcv6C0CopL2ukVzhzNw","UCe1Aj6VEO299Yq4WkXdoD3Q","UCZYTClx2T1of7BRZ86-8fow"]

    for cid in cids:
        add_channel(cid)

def get_all_channels():
    logging.info("called mf.get_all_channels")
    ccs = []
    for c in channels.find():
        ccs.append(c)

    return ccs

def test():
    vids = []
    for x in range(10):
        vids.append({'vid':x})

    videos.insert_many(vids,bypass_document_validation=True)