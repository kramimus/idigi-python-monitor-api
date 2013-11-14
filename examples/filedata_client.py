"""
FileData Client

NEEDS TO BE RUN WITH ACCESS TO https://github.com/digidotcom/idigi-python-monitor-api

based on push_client.py
"""
import argparse
import base64
import iso8601
import json
import logging
import os.path
import time
import zlib

from xml.dom.minidom import parseString
from idigi_monitor_api import push_client

LOG = logging.getLogger("filedata_client")

def json_cb(dest_root):
    """
    Sample callback, parses data as json and pretty prints it.
    Returns True if json is valid, False otherwise.

    :param data: The payload of the PublishMessage.
    """
    def callback(data):
        try:
            json_data = json.loads(data)
            for msg in json_data['Document']['Msg']:
                try:
                    if 'message' in msg['topic'] and 'FileData' in msg['topic']:
                        filedata = msg['FileData']
                        if 'id' in filedata and 'fdData' in filedata:
                            fileid = filedata.get('id')
                            modtime = iso8601.parse_date(filedata['fdLastModifiedDate'])
                            filepath = os.path.join(dest_root, fileid['fdPath'][1:], '%d/%d/%d' % (modtime.year, modtime.month, modtime.day))
                            filename = os.path.join(filepath, fileid['fdName'])

                            try:
                                os.makedirs(filepath)
                            except Exception:
                                pass
                            with open(filename, 'w') as f:
                                f.write(base64.b64decode(filedata['fdData']))

                            #LOG.info("Data Received %s" % json.dumps(msg, sort_keys=True,
                            #                                     indent=4))
                        LOG.info("Received FileData %s", msg['topic'])
                except Exception:
                    LOG.exception(str(msg))
            return True
        except Exception, exception:
            LOG.exception(data)

        return False
    return callback

def get_parser():
    """ Parser for this script """
    parser = argparse.ArgumentParser(description="iDigi Push Client Sample",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)

    parser.add_argument('username', type=str,
        help='Username to authenticate with.')

    parser.add_argument('password', type=str,
        help='Password to authenticate with.')

    parser.add_argument('--host', '-a', dest='host', action='store',
        type=str, default='my.idigi.com',
        help='iDigi server to connect to.')

    parser.add_argument('--insecure', dest='insecure', action='store_true',
        default=False,
        help='Prevent client from making secure (SSL) connection.')

    parser.add_argument('--compression', '-c',  dest='compression',
        action='store', type=str, default='gzip', choices=['none', 'gzip'],
        help='Compression type to use.')

    parser.add_argument('--batchsize', '-b', dest='batchsize', action='store',
        type=int, default=1,
        help='Amount of messages to batch up before sending data.')

    parser.add_argument('--batchduration', '-d', dest='batchduration',
        action='store', type=int, default=60,
        help='Seconds to wait before sending batch if batchsize not met.')

    parser.add_argument('--destination-root', dest='dest_root', type=str,
        required=True,
        help='Root directory to dump FileData files')

    return parser

def loop(args):
    logging.basicConfig(format='%(asctime)s %(levelname)s %(message)s',
                datefmt='%m/%d/%Y %I:%M:%S %p', level=logging.INFO)
    LOG.info("Creating Push Client.")

    client = push_client(args.username, args.password, hostname=args.host,
                        secure=not args.insecure)

    topics = ['FileData']

    LOG.info("Checking to see if Monitor Already Exists.")
    monitor_id = client.get_monitor(topics)

    # Delete Monitor if it Exists.
    if monitor_id is not None:
        LOG.info("Monitor already exists, deleting it.")
        client.delete_monitor(monitor_id)

    monitor_id = client.create_monitor(topics, format_type='json',
        compression=args.compression, batch_size=args.batchsize,
        batch_duration=args.batchduration)

    try:
        callback = json_cb(args.dest_root)
        client.create_session(callback, monitor_id)
        while True:
            time.sleep(.31416)
    except KeyboardInterrupt:
        # Expect KeyboardInterrupt (CTRL+C or CTRL+D) and print friendly msg.
        LOG.warn("Closing Sessions and Cleaning Up.")
    finally:
        client.stop_all()
        LOG.info("Deleting Monitor %s." % monitor_id)
        client.delete_monitor(monitor_id)
        LOG.info("Done")

def main():
    """ Main function call """
    args = get_parser().parse_args()
    loop(args)

if __name__ == "__main__":
    main()
