import mongo_funcs as mf
import youtube_extraction as yt
from log import logging

from datetime import datetime, timedelta

def populate():
    logging.info("called main.populate")
    for channel in mf.channels.find():
        logging.info("Getting videos for channel: " + channel['name'])
        playlistIds = yt.get_playlistitem_ids(channel['catalogId'], datetime.now() - timedelta(days=30))
        logging.info("Attempting to store {} videos".format(len(playlistIds['vids'])))
        res = mf.add_videos(playlistIds['vids'])
        logging.info("Stored {} videos".format(len(res)))

populate()