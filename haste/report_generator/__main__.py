import pymongo
import sys
import itertools
import pprint

BLANK_LINE_EVERY = 8
HEADER_LINE_EVERY = 40

# TODO: get from command line?
# SERVER_URI = 'mongodb://localhost:27017'
SERVER_URI = 'mongodb://metadata-db-prod:27017'

WELLS_FOR_ONLINE_ANALYSIS = ['B05', 'C02', 'C03', 'C04', 'C09', 'D04', 'D06', 'E10', 'F09', 'G02', 'G10', 'G11']
GREEN_COLOR_CHANNEL = 2

INTERESTINGNESS_GROUND_TRUTH = {'B05': 1, 'C02': 1, 'C03': 1, 'C04': 0, 'C09': 0, 'D04': 0,
                                'D06': 1, 'E10': 0, 'F09': 0, 'G02': 0, 'G10': 0, 'G11': 0}


def cols(cells):
    number_cols = 1 + len(WELLS_FOR_ONLINE_ANALYSIS)
    # right align, col of fixed width:
    pattern = '{:>5} ' * number_cols
    result = pattern.format(*cells)
    return result


def print_header():
    print(cols(['T'] + WELLS_FOR_ONLINE_ANALYSIS))


def print_ground_truth():
    print(cols(['GdTth'] + [INTERESTINGNESS_GROUND_TRUTH[well] for well in WELLS_FOR_ONLINE_ANALYSIS]))


client = pymongo.MongoClient(SERVER_URI)
stream_id = sys.argv[1]
if stream_id.startswith('strm_'):
    stream_id = stream_id[5:]
    print('Note: Ignoring the strm_ prefix - whilst used in the DB, it is not part of the stream ID!')

# TODO: default to most recent collection
collection = client.streams['strm_' + stream_id]

cursor = collection.find(filter={
    'substream_id': {'$in': WELLS_FOR_ONLINE_ANALYSIS},
    'metadata.imaging_point_number': 1,
    'metadata.color_channel': GREEN_COLOR_CHANNEL
},
    projection=['timestamp', 'interestingness', 'substream_id'])

results = list(cursor)

# Groupby only groups adjacent - need to sort first!
results = sorted(results, key=lambda doc: doc['timestamp'])
timestamp_groups = itertools.groupby(results, lambda x: x.get('timestamp'))

row_count = 0

legend = {'-': 'missing document',
          '1': 'interesting',
          '0.5': 'both interesting and uninteresting',
          '0.9': 'neither interesting nor uninteresting',
          'GdTth': 'ground truth interestingness',
          'T': 'timestamp'}

print('legend: ')
pprint.pprint(legend)
print()

for timestamp, timestamp_group in timestamp_groups:

    if row_count % BLANK_LINE_EVERY == 0:
        print()
    if row_count % HEADER_LINE_EVERY == 0:
        print_ground_truth()
        print_header()
        print()

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
