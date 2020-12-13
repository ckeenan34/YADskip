import mongo_funcs as mf
import youtube_extraction as yt
from log import logging

from datetime import datetime, timedelta
import pprint

def populate():
    logging.info("called main.populate")

    logging.info("!! Loading new channels from file")
    mf.load_channels_from_file()

    logging.info("!! Updating the channels")
    mf.update_channels()

    logging.info("!! Getting videos from channel playlist")
    for channel in mf.channels.find():
        logging.info("Getting videos for channel: " + channel['name'])

        dateCutoff = channel['dateCutoff'] - timedelta(days=1)
        (diff, ratio) = mf.video_count_diff(channel['cid'])
        if (ratio < .5 or diff > 50):
            logging.info("Too big of a difference, getting all videos for " + channel['name'])
            dateCutoff = datetime(1970,1,1)

        playlistVideos = yt.get_playlistitem_meta(channel['catalogId'],stopdate=dateCutoff)

        logging.info("Attempting to store {} videos".format(len(playlistVideos['videos'])))
        res = mf.bulk_upsert_videos(playlistVideos['videos'])

        logging.info("Stored {} videos".format(res))
    
    logging.info("!! SWEEEEEEEEPPPPPPPIIIIIINNNNNNNGGGGGG")
    mf.update_video_sweep()
    mf.add_captions_sweep()

    logging.info("!! Ignoring old captions")
    mf.ignore_empty_captions(30)

    logging.info("!! Updating the channels (again)")
    mf.update_channels()
        
if __name__ == "__main__":
    start_stats = mf.get_stats()
    logging.info("STARTIGN STATS: " + pprint.pformat(start_stats))

    populate()

    end_stats = mf.get_stats()
    logging.info("STARTIGN STATS (again): " + pprint.pformat(start_stats))
    logging.info("ENDING STATS: " + pprint.pformat(end_stats))