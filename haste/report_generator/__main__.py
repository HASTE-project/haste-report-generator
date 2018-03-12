import pymongo
import sys
import itertools
import pprint

BLANK_LINE_EVERY = 8
HEADER_LINE_EVERY = 40

# TODO: get from command line?
SERVER_URI = 'mongodb://localhost:27017'
# server_uri = 'mongodb://metadata-db-prod:27017'

WELLS_FOR_ONLINE_ANALYSIS = ['B05', 'C02', 'C03', 'C04', 'C09', 'D04', 'D06', 'E10', 'F09', 'G02', 'G10', 'G11']
GREEN_COLOR_CHANNEL = 2


def cols(cells):
    number_cols = 1 + len(WELLS_FOR_ONLINE_ANALYSIS)
    # right align, col of fixed width:
    pattern = '{:>5} ' * number_cols
    result = pattern.format(*cells)
    return result


def print_header():
    print(cols(['T'] + WELLS_FOR_ONLINE_ANALYSIS))


client = pymongo.MongoClient(SERVER_URI)
stream_id = sys.argv[1]
# TODO: default to most recent collection
collection = client.streams['strm_' + stream_id]

cursor = collection.find(filter={
    'substream_id': {'$in': WELLS_FOR_ONLINE_ANALYSIS},
    'metadata.imaging_point_number': 1,
    'metadata.color_channel': GREEN_COLOR_CHANNEL
},
    projection=['timestamp', 'interestingness', 'substream_id'])

results = list(cursor)

timestamp_groups = itertools.groupby(results, lambda x: x.get('timestamp'))

row_count = 0

legend = {'-': 'missing document'}

print('legend: ')
pprint.pprint(legend)
print()

for timestamp, timestamp_group in timestamp_groups:

    if row_count % BLANK_LINE_EVERY == 0:
        print()
    if row_count % HEADER_LINE_EVERY == 0:
        print_header()

    # TODO: verify that we start with T=1, and all T's are sequential

    intgness_by_sstream = {doc['substream_id']: doc['interestingness'] for doc in timestamp_group}

    # TODO:
    # - write reason to DB (and annotate with legend).

    cells = [timestamp] + list(
        map(lambda substream: intgness_by_sstream.get(substream, '- '),  # default for missing docs
            WELLS_FOR_ONLINE_ANALYSIS))

    print(cols(cells))

    row_count += 1

client.close()
