#!/usr/bin/env python
#
# Public Domain 2014-2015 MongoDB, Inc.
# Public Domain 2008-2014 WiredTiger, Inc.
#
# This is free and unencumbered software released into the public domain.
#
# Anyone is free to copy, modify, publish, use, compile, sell, or
# distribute this software, either in source code form or as a compiled
# binary, for any purpose, commercial or non-commercial, and by any
# means.
#
# In jurisdictions that recognize copyright laws, the author or authors
# of this software dedicate any and all copyright interest in the
# software to the public domain. We make this dedication for the benefit
# of the public at large and to the detriment of our heirs and
# successors. We intend this dedication to be an overt act of
# relinquishment in perpetuity of all present and future rights to this
# software under copyright law.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
# EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
# MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
# IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
# OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
# ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
# OTHER DEALINGS IN THE SOFTWARE.
#
# ex_backup.py
# 	demonstrates how to use incremental backup and log files.

import os
from __future__ import print_function
from wiredtiger import wiredtiger_open

home = "WT_HOME_LOG"
home_full = "WT_HOME_LOG_FULL"
home_incr = "WT_HOME_LOG_INCR"

full_out = "./backup_full"
incr_out = "./backup_incr"
uri = "table:logtest"

CONN_CONFIG = "create,cache_size=100MB,log=(archive=false,enabled=true,file_max=100K)"
MAX_ITERATIONS = 5
MAX_KEYS = 10000

def compare_backups(i):
	# We run 'wt dump' on both the full backup directory and the
	# incremental backup directory for this iteration.  Since running
	# 'wt' runs recovery and makes both directories "live", we need
	# a new directory for each iteration.
	#
	# If i == 0, we're comparing against the main, original directory
	# with the final incremental directory.
	if (i == 0):
		buf = "../../wt -R -h " + home + " dump logtest > " + full_out + "." + str(i)
	else:
		buf = "../../wt -R -h " + home_full + "." + str(i) +  " dump logtest > " + full_out + "." + str(i)
	os.system(buf)
	
	# Now run dump on the incremental directory.

	buf = "../../wt -R -h " + home_incr + "." + str(i) + " dump logtest > " + incr_out + "." + str(i)
	os.system(buf)
	
	# Compare the files.

	buf = "cmp " + full_out + "." + str(i) + " " + incr_out + "." + str(i)
	ret = os.system(buf)
	if (i == 0):
		msg = "MAIN"
	else:
		msg = str(i)
	print("Iteration " + msg + " Tables " + full_out + "." + i + " and " + incr_out + "." + i + " " + ("identical" if ret == 0 else "differ"))
	if (ret != 0):
		sys.exit(1)

	
	# If they compare successfully, clean up.
	
	if (i != 0):
		buf = "rm -rf " + home_full + "." + str(i) + " " \
			+ home_incr + "." + str(i) + " " \
			+ full_out + "." + str(i) + " " \
			+ incr_out + "." + str(i)
		os.system(buf)
	return

def setup_directories():
	
	# Set up all the directories needed for the test.  We have a full backup
	# directory for each iteration and an incremental backup for each iteration.
	# That way we can compare the full and incremental each time through.

	for i in range(0, MAX_ITERATIONS):
		# For incremental backups we need 0-N.  The 0 incremental
		# directory will compare with the original at the end.
		buf = "rm -rf " + home_incr + "." + str(i) + " && mkdir " + home_incr + "." + str(i)
		if ((ret = os.system(buf)) != 0):
			print(buf + ": failed ret " + str(ret), file=sys.stderr)
			return
		if (i == 0):
			continue
		
		# For full backups we need 1-N.
		
		buf = "rm -rf " + home_full + "." + str(i) + " && mkdir " + home_full + "." + str(i)
		if ((ret = os.system(buf)) != 0):
			print(buf + ": failed ret " + str(ret), file=sys.stderr)
			return
	return

def add_work(session, iter):
	#static int add_work(WT_SESSION *session, int iter)

	cursor = session.open_cursor(uri)
	# Perform some operations with individual auto-commit transactions.
	for i in range(0, MAX_KEYS):
		k = "key." + str(iter) + "." + str(i)
		v = "value." + str(iter) + "." + str(i)
		cursor[k] = v
		#cursor->set_key(cursor, k)
		#cursor->set_value(cursor, v)
		#ret = cursor->insert(cursor)
	cursor.close()

def take_full_backup(session, i):
	#static int take_full_backup(WT_SESSION *session, int i)

	# First time through we take a full backup into the incremental
	# directories.  Otherwise only into the appropriate full directory.
	
	if (i != 0):
		h = home_full + "." + str(i)
		hdir = h
	else:
		hdir = home_incr
	cursor = session.open_cursor("backup:")

	while cursor.next() == 0: # XXX Rewrite -- pgunn
		filename = cursor.get_key()
		if (i == 0):
			# Take a full backup into each incremental directory.
			for j in range(0, MAX_ITERATIONS):
				h = home_incr + "." + str(j)
				buf = "cp " + home + "/" + filename + " " + h + "/" + filename
				os.system(buf)
		else:
			h = home_full + "." + str(i)
			buf = "cp " + home + "/" + filename + " " + hdir + "/" + filename
			os.system(buf)

	if (ret != WT_NOTFOUND):	# FIXME Value should come from excised code in while/get_key above
		print("WT_CURSOR.next: " + session->strerror(session,ret), file=sys.stderr)
	cursor.close()

def take_incr_backup(session, i):
	# static int take_incr_backup(WT_SESSION *session, int i)
	WT_CURSOR *cursor
	const char *filename

	cursor = session.open_cursor("backup:", None, "target=(\"log:\")") # XXX is this the right way to do this?

	while cursor.next() == 0: # XXX Rewrite -- pgunn
		filename = cursor.get_key()
		
		# Copy into the 0 incremental directory and then each of the
		# incremental directories for this iteration and later.
		
		h = home_incr + ".0"
		buf = "cp " + home + "/" + filename + " " + h + "/" + filename
		os.system(buf)
		for j in range(i, MAX_ITERATIONS):
			h = home_incr + "." + str(j)
			buf = "cp " + home + "/" + filename + " " + h + "/" + filename
			os.system(buf)
	if (ret != WT_NOTFOUND):	# FIXME Value should come from excised code in while/get_key above
		print("WT_CURSOR.next: " + session->strerror(session,ret), file=sys.stderr)
	
	# With an incremental cursor, we want to truncate on the backup
	# cursor to archive the logs.  Only do this if the copy process
	# was entirely successful.
	
	cursor = session.truncate("log:")
	cursor.close()

def main():
	buf = "rm -rf " + home + " && mkdir " + home 
	if ((ret = os.system(cmd_buf)) != 0):
		print(cmd_buf + ": failed ret " + str(ret), file=sys.stderr)
		sys.exit(ret)
	try:
		wt_conn = wiredtiger_open(home, CONN_CONFIG):
	except:
		# TODO: How do we get the equivalent of "ret" from that open?
		print("Error connecting to " + home + ": " + wiredtiger_strerror(ret), file=sys.stderr)
		sys.exit(ret)

	setup_directories()
	session = wt_conn.open_session()
	session.create(uri, "key_format=S,value_format=S")
	print("Adding initial data")
	add_work(session, 0)

	print("Taking initial backup")
	take_full_backup(session, 0)

	session.checkpoint()

	for i in range(1, MAX_ITERATIONS):
		print("Iteration " + str(i) + ": adding data")
		add_work(session, i)
		session.checkpoint()
		
		# The full backup here is only needed for testing and
		# comparison purposes.  A normal incremental backup
		# procedure would not include this.
		
		print("Iteration " + str(i) + ": taking full backup")
		take_full_backup(session, i)
		
		# Taking the incremental backup also calls truncate
		# to archive the log files, if the copies were successful.
		# See that function for details on that call.
		
		print("Iteration " + str(i) + ": taking incremental backup")
		take_incr_backup(session, i)

		print("Iteration " + str(i) + ": dumping and comparing data")
		compare_backups(i)
	
	# Close the connection.  We're done and want to run the final
	# comparison between the incremental and original.
	
	wt_conn.close()
	print("Final comparison: dumping and comparing data")
	compare_backups(0)

if __name__ == "__main__":
    main()

