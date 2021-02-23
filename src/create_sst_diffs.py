from pyrocksdb import DB, Options, ReadOptions, SstFileWriter, WriteOptions,\
        EnvOptions, Env, ExternalSstFileInfo, IngestExternalFileOptions
from cassandra.cluster import Cluster
from cassandra.query import ordered_dict_factory, SimpleStatement
from cassandra.concurrent import execute_concurrent
import logging
import argparse
import bisect
import json
import os
import traceback
import sys
import tempfile
from datetime import datetime

c_max = 0
batchsize = 10000

logformat = '%(asctime)-15s %(message)s'
loglevel = logging.INFO

logging.basicConfig(format=logformat)
logger = logging.getLogger()
logger.setLevel(loglevel)


sst_diffs_path = '/sst_diffs'

keyspace_prefix_sep = ':'


def check_keyspace(arg):
    if keyspace_prefix_sep not in arg:
        prefix = keyspace = arg
    else:
        split = arg.split(keyspace_prefix_sep)
        if len(split) != 2 or len(split[1]) == 0:
            raise argparse.ArgumentTypeError("Invalid argument: %s" % arg)
        prefix = split[0]
        keyspace = split[1]
    return prefix, keyspace

parser = argparse.ArgumentParser(description='''
Create sst files diffing cassandra and rocksdb.''')

parser.add_argument('sst_dir', type=str,
                    help='subdirectory in {} with existing sst files'
                         .format(sst_diffs_path))
parser.add_argument('keyspace', type=str,
                    help='keyspace to grab')
parser.add_argument('--table', type=str,
                    help='optionally the table to grab')
parser.add_argument('nodes', type=str, nargs='+',
                    help='cassandra nodes')

args = parser.parse_args()
print(args)

logger.info("Opening new Cassandra cluster connection: {}".format(args.nodes))
cluster = Cluster(args.nodes)
session = cluster.connect()
session.row_factory = ordered_dict_factory

read_opts = ReadOptions()
write_opts = WriteOptions()
env = EnvOptions()
opts = Options()
iopts = IngestExternalFileOptions()

sst_file_writer = SstFileWriter(env, opts, None, True,
                                Env.IO_TOTAL, False)

now = datetime.now()
format = "%Y%m%d-%H%M%S"
timestamp = now.strftime(format)


def format_exception(e):
    exception_list = traceback.format_stack()
    exception_list = exception_list[:-2]
    exception_list.extend(traceback.format_tb(sys.exc_info()[2]))
    exception_list.extend(
        traceback.format_exception_only(sys.exc_info()[0], sys.exc_info()[1]))

    exception_str = "Traceback (most recent call last):\n"
    exception_str += "".join(exception_list)
    # Removing the last \n
    exception_str = exception_str[:-1]

    return exception_str


def get_tables(keyspace):
    tables = cluster.metadata.keyspaces[keyspace].tables
    return sorted(tables.keys())


def to_bytes(string):
    return string.encode('utf-8')


number_format = '%020d'
key_separator = ' '


def make_key(table, pks, row):
    key = [table]
    for pk in pks:
        if 'int' in pk.cql_type:
            k = number_format % row[pk.name]
        elif pk.cql_type == 'blob':
            k = row[pk.name].hex()
        else:
            k = str(row[pk.name])
        key.append(k)
    return to_bytes(' '.join(key))


def get_key(table, *args):
    key = []
    for arg in args:
        if isinstance(arg, int):
            k = number_format % arg
        elif isinstance(arg, bytes):
            k = arg.hex()
        else:
            k = str(arg)
        key.append(k)
    return to_bytes(key_separator.join(key))


def make_value(row):
    return to_bytes(json.dumps(row, default=lambda o: o.hex()))


def make_sst_filename(keyspace):
    return "%s_%s.sst" % (keyspace, timestamp)


def get_partitions(table, pk):
    query = "SELECT distinct %s FROM %s" % (pk, table)
    statement = SimpleStatement(query)
    paging_state = True
    values = []
    while paging_state:
        if paging_state is True:
            paging_state = None
        result = session.execute(statement, paging_state=paging_state)
        for row in result.current_rows:
            bisect.insort(values, row[pk])
        paging_state = result.paging_state
    return values


def diffdump_table(db, sst_file_writer, keyspace, table):
    if args.table and args.table != table:
        logger.info('Omitting %s' % table)
        return 0
    logger.info("Diff-dumping %s" % table)
    pks = cluster.metadata.keyspaces[keyspace].tables[table].primary_key
    logger.info('Lookup partitions...')
    logger.debug('type pk {}'.format(pks[0].cql_type))
    order_keys = ','.join([(pk.name + " ASC") for pk in pks[1:]])
    if order_keys:
        order_keys = "ORDER BY " + order_keys

    partitions = get_partitions(table, pks[0].name)
    logger.info('Found %i partitions' % len(partitions))
    statement = session.prepare(
                    "SELECT * FROM %s WHERE %s=? %s"
                    % (table, pks[0].name, order_keys))
    for partition in partitions:
            logger.debug('partition {}'.format(partition))
    for j in range(0, len(partitions), batchsize):
        k = j+batchsize
        statements_and_params = ((statement,
                                  (int(partition)
                                   if 'int' in pks[0].cql_type
                                   else partition,))
                                 for partition in partitions[j:k])
        c = 0
        logger.info('Query cassandra for partitions {} to {} concurrently'
                    ' and write to sst file...'.format(j, k))
        results = execute_concurrent(session,
                                     statements_and_params,
                                     raise_on_first_error=True,
                                     results_generator=True)
        i = 0
        for (success, result) in results:
            if not success:
                logger.error('ERROR retrieving result: {}'
                             .format(format_exception(result)))
                continue
            for row in result:
                if c_max > 0 and c > c_max:
                    break
                c += 1
                key = make_key(table, pks, row)
                value = make_value(row)
                current = db.get(read_opts, key)
                logger.debug("key %s, value %s" % (key, value))
                logger.debug("current %s" % current.data)

                if current.status.ok() and current.data == value:
                    continue
                logger.debug('insert/update')
                i += 1
                sst_file_writer.put(key, value)
    logger.info('Found %i diffs for %s.%s' % (i, keyspace, table))
    return i


def diffdump(sst_dir, keyspace):
    sst_dir = os.path.join(sst_diffs_path, sst_dir)
    logger.info("Diffing %s against sst files from %s"
                % (keyspace, sst_dir))
    session.set_keyspace(keyspace)
    db = DB()
    opts = Options()
    # for multi-thread
    opts.IncreaseParallelism()
    opts.OptimizeLevelStyleCompaction()
    opts.create_if_missing = True
    with tempfile.TemporaryDirectory() as db_path:
        path = os.path.join(db_path, keyspace)
        logger.debug('db_path {}'.format(path))
        s = db.open(opts, path)
        if not s.ok():
            raise RuntimeError("Could not open db %s" % path)
        if not os.path.exists(sst_dir):
            os.mkdir(sst_dir)
        files = [os.path.join(sst_dir, f) for f in os.listdir(sst_dir)]
        if len(files) > 0:
            logger.info('Ingesting sst files from %s...' % sst_dir)
            for f in files:
                s = db.ingest_external_file([f], iopts)
                if not s.ok():
                    if s.is_corruption():
                        os.remove(f)
                        continue
                    raise RuntimeError("Could not ingest %s, code: %s"
                                       % (files, s.code()))
        """
        opts = ReadOptions()
        it = db.iterator(opts)
        it.seek_to_first()
        while it.valid():
            print("{} -> {}".format(it.key(), it.value()))
            it.next()
        """
        sst_filename = make_sst_filename(keyspace)
        sst_filename = os.path.join(sst_dir, sst_filename)
        sst_file_status = sst_file_writer.open(sst_filename)
        if not sst_file_status.ok():
            raise RuntimeError("Could not open sst file %s"
                               % sst_filename)
        try:
            i = 0
            for table in get_tables(keyspace):
                i += diffdump_table(db, sst_file_writer, keyspace, table)
            file_info = ExternalSstFileInfo()
            sst_file_writer.finish(file_info)
            if not file_info.smallest_key and file_info.smallest_key is not 0:
                os.remove(sst_filename)
                logger.info('No changes found in %s' % (keyspace))
                return
            logger.info(('Dumped %i diffs of %s to %s '
                         '(smallest key: %s, largest key: %s)')
                        % (i, keyspace, sst_filename,
                           file_info.smallest_key,
                           file_info.largest_key))
        except Exception as e:
            os.remove(sst_filename)
            raise e

        finally:
            db.close()


try:
    diffdump(args.sst_dir, args.keyspace)
except Exception as e:
    logger.error("ERROR: %s" % format_exception(e))
finally:
    cluster.shutdown()
