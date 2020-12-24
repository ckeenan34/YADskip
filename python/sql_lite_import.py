import mongo_funcs as mf
import youtube_extraction as yt 

import sqlite3

sponsorBlockDB = "../data/database.db"

def create_connection(db_file):

    conn = None
    try:
        conn = sqlite3.connect(db_file, timeout=20, isolation_level="EXCLUSIVE", cached_statements=500)
    except Exception as e:
        print(e)

    return conn

def getdb():
    return create_connection(sponsorBlockDB)

def fetch_one(query):
    with getdb() as conn:
        return conn.cursor().execute(query).fetchone()

def import_all_videos():

    with getdb() as conn:
        cur = conn.cursor()

        cur.execute("SELECT DISTINCT videoID FROM sponsorTimes ORDER BY videoID DESC")

        count = 0
        vids = []
        for (vid,) in cur:
            vids.append({"vid":vid})
            count +=1
            if len(vids)>= 5000:
                mf.bulk_upsert_videos(vids)
                vids = []
                print("### On video #" + str(count))
        mf.bulk_upsert_videos(vids)

def import_segments():
    # this is all fucked up because someone *cough* *cough* SQLLITE DRIVER couldn't handle
    # getting only 700,000 rows of data back without crashing/corrupting the .db file

    rowCount = fetch_one("SELECT COUNT(*) FROM sponsorTimes")[0]
    current_index = 0
    step = 100
    segments = {}
    attemps = 1
    last_index = 0

    while current_index < rowCount:
        try:
            if (current_index % 5000 == 0):
                print("Current index: {}".format(current_index))
            with getdb() as conn:
                cur = conn.cursor()
                cur.execute("""
                                SELECT videoID,startTime,endTime,votes,views,category 
                                FROM sponsorTimes 
                                ORDER BY videoID ASC 
                                LIMIT {},{}
                            """.format(current_index,step))
                for row in cur:
                    entry = {
                        "startTime": row[1],
                        "endTime": row[2],
                        "votes": row[3],
                        "views": row[4],
                        "category": row[5]
                    }
                    if row[0] in segments:
                        segments[row[0]] += [entry]
                    else:
                        segments[row[0]] = [entry]
                current_index += step
                last_index = current_index
        except:
            print("There was an exception on index {}".format(current_index))
            if (last_index == current_index):
                attemps +=1

            if attemps > 5:
                print("Index of {} failed more than 5 times, moving on".format(current_index))
                current_index += step
                last_index = current_index
                attemps=0
            continue

    segments_to_mongo(segments) 

def build_indecies(starts, jump, step):
    indecies = []
    for s in starts:
        indecies += list(range(s, s+jump, step))

    return indecies

def fix_broken_segments():
    # this is to fix those shitty segments that are causing the failures

    step = 1
    segments = {}
    attemps = 1
    last_index = 0
    indecies = build_indecies([226400,365000,636400],100,step) + [636500]
    # These are the problem childs
    # 226400
    # 365000
    # 636400

    index = 0
    while index < len(indecies):
        real_index = indecies[index]
        try:
            if (real_index % 20 == 0):
                print("Current real_index: {}".format(real_index))
            with getdb() as conn:
                cur = conn.cursor()
                cur.execute("""
                                SELECT videoID,startTime,endTime,votes,views,category 
                                FROM sponsorTimes 
                                ORDER BY videoID ASC 
                                LIMIT {},{}
                            """.format(real_index,step))
                for row in cur:
                    entry = {
                        "startTime": row[1],
                        "endTime": row[2],
                        "votes": row[3],
                        "views": row[4],
                        "category": row[5]
                    }
                    if row[0] in segments:
                        segments[row[0]] += [entry]
                    else:
                        segments[row[0]] = [entry]
                index+=1
                last_index = index
        except:
            print("There was an exception on real_index {}".format(real_index))
            if (last_index == index):
                attemps +=1

            if attemps > 5:
                print("real_index of {} failed more than 5 times, moving on".format(real_index))
                index+=1
                last_index = index
                attemps=0
            continue

    return segments
# 226400
# 365000
# 636400

def segments_to_mongo(segments):
    batch = []
    for key in segments:
        batch.append({
            "vid": key,
            "segments": segments[key]
        })

    mf.bulk_upsert_videos(batch)

# batch = []
# def segment_to_mongo(vid, segments, force=False):
#     global batch
#     batch += [{
#         "vid": vid,
#         "segments": segments
#     }]

#     if len(batch) > 2000 or force:
#         mf.bulk_upsert_videos(batch)
#         batch = []

if __name__ == "__main__":
    pass
    # import_all_videos()





# segments = {}
# for row in cur:
#     entry = {
#         "startTime": row[1],
#         "endTime": row[2],
#         "votes": row[3],
#         "views": row[4],
#         "category": row[5]
#     }
#     if row[0] in segments:
#         segments[row[0]] += [entry]
#     else:
#         segments[row[0]] = [entry]



# tmp = []
# vid = ""
# for row in cur:
#     if row[0] != vid:
#         segment_to_mongo(vid, tmp)
#         tmp = []

#     vid = row[0]
#     entry = {
#         "startTime": row[1],
#         "endTime": row[2],
#         "votes": row[3],
#         "views": row[4],
#         "category": row[5]
#     }
#     tmp.append(entry)

# segment_to_mongo(vid, tmp, True)