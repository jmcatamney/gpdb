import os
import pipes
import tempfile

from behave import given, then
from pygresql import pg

from gppylib.commands.base import Command, REMOTE
from gppylib.db import dbconn
from gppylib.gparray import GpArray
from test.behave_utils.utils import run_cmd, wait_for_unblocked_transactions

class Tablespace:
    def __init__(self, name):
        self.name = name
        self.path = tempfile.mkdtemp()
        self.dbname = 'tablespace_db_%s' % name
        self.table_counter = 0
        self.initial_data = None

        gparray = GpArray.initFromCatalog(dbconn.DbURL())
        for host in gparray.getHostList():
            run_cmd('ssh %s mkdir -p %s' % (pipes.quote(host), pipes.quote(self.path)))

        with dbconn.connect(dbconn.DbURL(), unsetSearchPath=False) as conn:
            db = pg.DB(conn)
            db.query("CREATE TABLESPACE %s LOCATION '%s'" % (self.name, self.path))
            db.query("CREATE DATABASE %s TABLESPACE %s" % (self.dbname, self.name))

        with dbconn.connect(dbconn.DbURL(dbname=self.dbname), unsetSearchPath=False) as conn:
            db = pg.DB(conn)
            db.query("CREATE TABLE tbl (i int) DISTRIBUTED RANDOMLY")
            db.query("INSERT INTO tbl VALUES (GENERATE_SERIES(0, 25))")
            # save the distributed data for later verification
            self.initial_data = db.query("SELECT gp_segment_id, i FROM tbl").getresult()

    def cleanup(self):
        with dbconn.connect(dbconn.DbURL(dbname="postgres"), unsetSearchPath=False) as conn:
            db = pg.DB(conn)
            db.query("DROP DATABASE IF EXISTS %s" % self.dbname)
            db.query("DROP TABLESPACE IF EXISTS %s" % self.name)

            # Without synchronous_commit = 'remote_apply' introduced in 9.6, there
            # is no guarantee that the mirrors have removed their tablespace
            # directories by the time the DROP TABLESPACE command returns.
            # We need those directories to no longer be in use by the mirrors
            # before removing them below.
            _checkpoint_and_wait_for_replication_replay(db)

        gparray = GpArray.initFromCatalog(dbconn.DbURL())
        for host in gparray.getHostList():
            run_cmd('ssh %s rm -rf %s' % (pipes.quote(host), pipes.quote(self.path)))

    def verify(self, hostname=None, port=0):
        """
        Verify tablespace functionality by ensuring the tablespace can be
        written to, read from, and the initial data is still correctly
        distributed.
        """
        url = dbconn.DbURL(hostname=hostname, port=port, dbname=self.dbname)
        with dbconn.connect(url, unsetSearchPath=False) as conn:
            db = pg.DB(conn)
            data = db.query("SELECT gp_segment_id, i FROM tbl").getresult()

            # verify that we can still write to the tablespace
            self.table_counter += 1
            db.query("CREATE TABLE tbl_%s (i int) DISTRIBUTED RANDOMLY" % self.table_counter)
            db.query("INSERT INTO tbl_%s VALUES (GENERATE_SERIES(0, 25))" % self.table_counter)

        if sorted(data) != sorted(self.initial_data):
            raise Exception("Tablespace data is not identically distributed. Expected:\n%r\n but found:\n%r" % (
                sorted(self.initial_data), sorted(data)))

    def verify_for_gpexpand(self, hostname=None, port=0):
        """
        For gpexpand, we need make sure:
          1. data is the same after redistribution finished
          2. the table's numsegments is enlarged to the new cluster size
        """
        url = dbconn.DbURL(hostname=hostname, port=port, dbname=self.dbname)
        with dbconn.connect(url, unsetSearchPath=False) as conn:
            db = pg.DB(conn)
            data = db.query("SELECT gp_segment_id, i FROM tbl").getresult()
            tbl_numsegments = dbconn.execSQLForSingleton(conn,
                                                         "SELECT numsegments FROM gp_distribution_policy "
                                                         "WHERE localoid = 'tbl'::regclass::oid")
            num_segments = dbconn.execSQLForSingleton(conn,
                                                     "SELECT COUNT(DISTINCT(content)) - 1 FROM gp_segment_configuration")

        if tbl_numsegments != num_segments:
            raise Exception("After gpexpand the numsegments for tablespace table 'tbl' %d does not match "
                            "the number of segments in the cluster %d." % (tbl_numsegments, num_segments))

        initial_data = [i for _, i in self.initial_data]
        data_without_segid = [i for _, i in data]
        if sorted(data_without_segid) != sorted(initial_data):
            raise Exception("Tablespace data is not identically distributed after running gp_expand. "
                            "Expected pre-gpexpand data:\n%\n but found post-gpexpand data:\n%r" % (
                                sorted(self.initial_data), sorted(data)))


def _checkpoint_and_wait_for_replication_replay(db):
    """
    Taken from src/test/walrep/sql/missing_xlog.sql
    """
    db.query("""
-- checkpoint to ensure clean xlog replication before bring down mirror
create or replace function checkpoint_and_wait_for_replication_replay (retries int) returns bool as
$$
declare
	i int;
	checkpoint_locs pg_lsn[];
	replay_locs pg_lsn[];
	failed_for_segment text[];
	r record;
	all_caught_up bool;
begin
	i := 0;

	-- Issue a checkpoint.
	checkpoint;

	-- Get the WAL positions after the checkpoint records on every segment.
	for r in select gp_segment_id, pg_current_xlog_location() as loc from gp_dist_random('gp_id') loop
		checkpoint_locs[r.gp_segment_id] = r.loc;
	end loop;
	-- and the QD, too.
	checkpoint_locs[-1] = pg_current_xlog_location();

	-- Force some WAL activity, to nudge the mirrors to replay past the
	-- checkpoint location. There are some cases where a non-transactional
	-- WAL record is created right after the checkpoint record, which
	-- doesn't get replayed on the mirror until something else forces it
	-- out.
	drop table if exists dummy;
	create temp table dummy (id int4) distributed randomly;

	-- Wait until all mirrors have replayed up to the location we
	-- memorized above.
	loop
		all_caught_up = true;
		for r in select gp_segment_id, replay_location as loc from gp_stat_replication loop
			replay_locs[r.gp_segment_id] = r.loc;
			if r.loc < checkpoint_locs[r.gp_segment_id] then
				all_caught_up = false;
				failed_for_segment[r.gp_segment_id] = 1;
			else
				failed_for_segment[r.gp_segment_id] = 0;
			end if;
		end loop;

		if all_caught_up then
			return true;
		end if;

		if i >= retries then
			RAISE INFO 'checkpoint_locs:    %', checkpoint_locs;
			RAISE INFO 'replay_locs:        %', replay_locs;
			RAISE INFO 'failed_for_segment: %', failed_for_segment;
			return false;
		end if;
		perform pg_sleep(0.1);
		i := i + 1;
	end loop;
end;
$$ language plpgsql;

SELECT checkpoint_and_wait_for_replication_replay(0);
DROP FUNCTION checkpoint_and_wait_for_replication_replay(int);
    """)


@given('a tablespace is created with data')
def impl(context):
    _create_tablespace_with_data(context, "outerspace")


@given('another tablespace is created with data')
def impl(context):
    _create_tablespace_with_data(context, "myspace")


@given('three tablespaces are created with data')
def impl(context):
    _create_tablespace_with_data(context, "space_one")
    _create_tablespace_with_data(context, "space_two")
    _create_tablespace_with_data(context, "space_three")



def _create_tablespace_with_data(context, name):
    if 'tablespaces' not in context:
        context.tablespaces = {}
    context.tablespaces[name] = Tablespace(name)


@then('the tablespace is valid')
def impl(context):
    context.tablespaces["outerspace"].verify()


@then('all three tablespaces are valid')
def impl(context):
    context.tablespaces["space_one"].verify()
    context.tablespaces["space_two"].verify()
    context.tablespaces["space_three"].verify()


@then('the tablespace is valid on the standby master')
def impl(context):
    context.tablespaces["outerspace"].verify(context.standby_hostname, context.standby_port)


@then('the other tablespace is valid')
def impl(context):
    context.tablespaces["myspace"].verify()


@then('the tablespace is valid after gpexpand')
def impl(context):
    for _, tbs in context.tablespaces.items():
        tbs.verify_for_gpexpand()

@then('all tablespaces are dropped')
def impl(context):
    for tablespace in context.tablespaces.values():
        tablespace.cleanup()
    context.tablespaces = {}


@given('the symlink for tablespace {name} is set to a different value for the {seg_type} segment for content {content}')
def impl(context, name, seg_type, content):
    change_tablespace_location(context, name, seg_type, content, reset=False)

@then('the symlink for tablespace {name} is reset for the {seg_type} segment for content {content}')
def impl(context, name, seg_type, content):
    change_tablespace_location(context, name, seg_type, content, reset=True)


def change_tablespace_location(context, name, seg_type, content, reset=False):
    st = None
    if seg_type in ["primary", "mirror"]:
        st = seg_type[0]
    else:
        raise Exception("Invalid segment type: %s.  Valid types are primary and mirror." % seg_type)

    with dbconn.connect(dbconn.DbURL(), unsetSearchPath=False) as conn:
        dbid, host, datadir = dbconn.execSQLForSingletonRow(conn,
            "SELECT dbid, hostname, datadir FROM gp_segment_configuration WHERE content = %s and preferred_role = '%s'" % (content, st))
        oid = dbconn.execSQLForSingleton(conn, "SELECT oid FROM pg_tablespace WHERE spcname = '%s'" % name)
        standard_loc = dbconn.execSQLForSingleton(conn,
            "SELECT tblspc_loc FROM gp_tablespace_location(%s) WHERE gp_segment_id=%s" % (oid, content))
    different_loc = "%s_new" % standard_loc
    if reset:
        old_loc, new_loc = different_loc, standard_loc
    else:
        old_loc, new_loc = standard_loc, different_loc
        if "tablespace_mappings" not in context:
            context.tablespace_mappings = {}
        context.tablespace_mappings[content] = [old_loc, new_loc]

    cmd = Command("set tablespace location", cmdStr = '''cd %s/pg_tblspc;
            OID=%s;
            OLD_LOC=%s;
            NEW_LOC=%s;
            mkdir -p \$NEW_LOC;
            mv \$OLD_LOC/%s \$NEW_LOC/%s;
            ln -s \$NEW_LOC/%s newlink;
            rm \$OID;
            mv newlink \$OID;
            ''' % (datadir, oid, old_loc, new_loc, dbid, dbid, dbid), ctxt=REMOTE, remoteHost=host)
    cmd.run(validateAfter=True)

@given('the {seg_type} segment for content {content} is killed')
def impl(context, seg_type, content):
    st = None
    if seg_type in ["primary", "mirror"]:
        st = seg_type[0]
    else:
        raise Exception("Invalid segment type: %s.  Valid types are primary and mirror." % seg_type)

    with dbconn.connect(dbconn.DbURL(), unsetSearchPath=False) as conn:
        host, port = dbconn.execSQLForSingletonRow(conn,
            "SELECT hostname, port FROM gp_segment_configuration WHERE content = %s and preferred_role = '%s'" % (content, st))
    cmd = Command("kill segment", cmdStr="ps aux | grep '\-p %s' | grep -v grep | awk '{print \$2}' | xargs kill -9" % port, ctxt=REMOTE, remoteHost=host)
    cmd.run(validateAfter=True)
    wait_for_unblocked_transactions(context)

@given('a tablespace map file is created')
def impl(context):
    if "tablespace_mappings" not in context:
        raise Exception("No mismatched segments exist, a tablespace mapping file cannot be created")
    with open('/tmp/tablespace_map_file', 'w') as fd:
        for content in context.tablespace_mappings:
            old_loc, new_loc = context.tablespace_mappings[content]
            fd.write('%s:%s=%s\n' % (content, old_loc, new_loc))
        fd.write('42:/tmp/foo=/tmp/bar') # write a line for a nonexistent dbid to ensure gprecoverseg doesn't error out on that

@then('tablespace {name} for the {seg_type} segment for content {content} is recovered to its nonstandard location')
def impl(context, name, seg_type, content):
    st = None
    if seg_type in ["primary", "mirror"]:
        st = seg_type[0]
    else:
        raise Exception("Invalid segment type: %s.  Valid types are primary and mirror." % seg_type)

    if "tablespace_mappings" not in context:
        raise Exception("No mismatched segments exist, the location of %s on %s %s cannot be checked" % (name, seg_type, content))

    with dbconn.connect(dbconn.DbURL(), unsetSearchPath=False) as conn:
        dbid, host, datadir = dbconn.execSQLForSingletonRow(conn,
            "SELECT dbid, hostname, datadir FROM gp_segment_configuration WHERE content = %s and preferred_role = '%s'" % (content, st))
        oid = dbconn.execSQLForSingleton(conn, "SELECT oid FROM pg_tablespace WHERE spcname='%s'" % name)

    nonstandard_loc = "%s/%s" % (context.tablespace_mappings[content][1], dbid)
    cmd = Command('check tablespace location', cmdStr="readlink %s/pg_tblspc/%s" % (datadir, oid), ctxt=REMOTE, remoteHost=host)
    cmd.run(validateAfter=True)
    tablespace_path = cmd.get_results().stdout.strip()
    if tablespace_path != nonstandard_loc:
        raise Exception("Expected tablespace location %s, got location %s" % (nonstandard_loc, tablespace_path))
