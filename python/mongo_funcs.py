from pymongo import MongoClient, UpdateOne
from bson.objectid import ObjectId
from pymongo.errors import BulkWriteError

import youtube_extraction as yt 
from log import logging

from datetime import datetime, timedelta
import csv

# client = MongoClient('mongodb://admin:<password>@192.168.0.18:27017')
client = MongoClient('mongodb://localhost:27017/?connectTimeoutMS=30000&maxIdleTimeMS=600000')
db=client.YADskip

channels = db.channels
videos = db.videos

attempts_threshold = 5
inserting_threshold = 20
sweep_limit = 0 # no limit

# helpful dictionaries to use in mongo queries 
no_captions = {"$or": [{"captions": {"$exists": False}},{"captions": {"$size": 0}},{"captions": None}]}
try_for_captions = no_captions.copy()
try_for_captions["captionAttemps"] = {"$lt": attempts_threshold}

too_old_for_captions = try_for_captions.copy()
too_old_for_captions["publishedAt"] = {"$lt":  datetime.now() - timedelta(days=30)}
has_caps = {"$nor": [
    {"captions": {"$exists": False}},
    {"captions": {"$size": 0}},
    {"captions": None}
]}


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
    cmeta['checkAgain'] = datetime.utcnow() + timedelta(hours=23)

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

# get missing meta data (except captions) from videos that were added with the playlist items
def update_video_sweep():
    logging.info("called update_video_sweep")
    cursor = videos.find({"$or": [{"duration": {"$exists": False}},{"categoryId": {"$exists": False}}]}).limit(sweep_limit)
    
    result = 0
    vids = []
    x= 0
    for video in cursor:
        vids.append(video['vid'])
        x+=1
        if (x >= inserting_threshold):
            logging.info("getting meta data for #{} videos".format(len(vids)))
            video_metas = yt.get_video_metas(",".join(vids))
            result += bulk_upsert_videos(video_metas)

            vids = []
            x= 0

    if (len(vids) > 0):
        logging.info("getting meta data for #{} videos".format(len(vids)))
        video_metas = yt.get_video_metas(",".join(vids))
        result += bulk_upsert_videos(video_metas)

    return result

# sweep through all existing videos and get captions for any that don't have them and are under the attempts threshold
def add_captions_sweep():
    logging.info("called add_captions_sweep")
    cursor = videos.find(try_for_captions).limit(sweep_limit)
    
    result = 0
    caption_updates = []
    x= 0
    for video in cursor:
        print(video['vid'])
        cap = yt.get_captions(video['vid'])
        caption_updates.append({
            "vid": video['vid'],
            "captionAttemps": video["captionAttemps"] +1 if "captionAttemps" in video else 1,
            "captions": cap,
            "hasCaptions": cap != None
        })
        x+=1
        if x>= inserting_threshold:
            result += bulk_upsert_videos(caption_updates)
            caption_updates = []
            x = 0

    if (len(caption_updates) > 0):
        result += bulk_upsert_videos(caption_updates)

    return result

def bulk_upsert_videos(bulk_videos):
    logging.info("called bulk_upsert_videos on #{} vids".format(len(bulk_videos)))
    
    if (len(bulk_videos) == 0):
        return 0
    
    operations = []
    for video in bulk_videos:
        operations.append(UpdateOne({"vid": video["vid"]},{"$set": video}, upsert=True))
    
    try:
        result = videos.bulk_write(operations)
    except BulkWriteError as bwe:
        print(bwe.details)
        return 0
    
    return result.matched_count

def bulk_upsert_channels(bulk_channels):
    logging.info("called bulk_upsert_channels on #{} channels".format(len(bulk_channels)))
    
    if (len(bulk_channels) == 0):
        return 0

    operations = []
    for channel in bulk_channels:
        operations.append(UpdateOne({"cid": channel["cid"]},{"$set": channel}, upsert=True))
    
    try:
        result = channels.bulk_write(operations)
    except BulkWriteError as bwe:
        print(bwe.details)
        raise
    
    return result.matched_count

def update_channels():
    logging.info("called mf.update_channels")
    cids = set() # to add a channel, put the ID in this set 

    for channel in channels.find():
        cids.add(channel['cid'])

    all_channels = []
    for cid in cids:
        meta = yt.get_channel_meta(cid)
        meta['dateCutoff'] = get_date_cutoff(meta['cid'])
        # logging.info("date cutoff for {} is: {}".format(cid,meta['dateCutoff']))
        # meta['checkAgain'] = datetime.utcnow() = timedelta(hours=23)
        all_channels.append(meta)

    return bulk_upsert_channels(all_channels)

def get_date_cutoff(cid):
    recent_video = videos.find_one({"channelId":cid, "captions": {"$not": {"$size": 0}}}, sort=[("publishedAt",-1)])
    if (recent_video):
        return recent_video['publishedAt']
    else: 
        return datetime(1970,1,1)

def video_count_diff(cid):
    channel = channels.find_one({"cid":cid})
    ccount = int(channel["videoCount"])

    vcount = videos.find({"channelId":cid}).count()

    diff = ccount - vcount 
    ratio = vcount/ccount if ccount > 0 else 1

    return (diff, ratio)

def get_stats(should_print=False):
    cursor = videos.find({"captions": None})

    cvc = {}
    for channel in channels.find():
        cvc[channel['name']] = videos.find({"channelId": channel['cid']}).count()


    
    final_stats = {
        'video_count': videos.find().count(),
        'captions_count': videos.find(has_caps).count(),
        'no_captions_count':videos.find(no_captions).count(),
        'missing_metadata_count': videos.find({"$or": [{"duration": {"$exists": False}},{"categoryId": {"$exists": False}}]}).count(),
        # 'failed_attempts_avg':,
        'lt_attempts_threshold_count': videos.find(try_for_captions).count(),
        'channel_count': channels.find().count(),
        'channel_video_counts':cvc,
        'channel_recent_velo':{}
    }

    if (should_print):
        for key in final_stats:
            print("{}: {}".format(key,final_stats[key]))

    return final_stats

def load_channels_from_file():
    final = []
    with open('../data/cids.csv',mode='r') as cids:
        for line in csv.DictReader(cids):
            final.append(line)

    bulk_upsert_channels(final)

# Overwrite old (>diff days old) videos without captions so they go 
# over the treshold and wont be checked for captions anymore
def ignore_empty_captions(diff=30):
    videos.update_many(too_old_for_captions, {"$set": {"captionAttemps": attempts_threshold}})
