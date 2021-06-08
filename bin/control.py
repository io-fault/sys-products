"""
# Product build and configuration system command interface.
"""
import sys
import os
import typing

from fault.system import files
from fault.system import process
from fault.project import root
from fault.context import tools

from .. import operations

def _no_argv(argv):
	if argv:
		sys.stderr.write("WARNING: issued command does not take arguments.\n")

def clean(pd:root.Product, argv=(), **ignored):
	"""
	# Remove factor image files.
	"""
	sys.stderr.write("NOTICE: no effect; clean currently not implemented.\n")
	return 0

# Rebuild project index from directory structure.
def index(pd:root.Product, argv=(), **ignored):
	"""
	# Create or update the project index by scanning the filesystem.
	"""
	_no_argv(argv)

	operations.update(pd)
	sys.stderr.write("NOTICE: updated project index using the product directory.\n")
	return 0

def connect(pd:root.Product, position=None, contexts=None, argv=()):
	"""
	# Add connections to product index.
	"""

	targets = [files.Path.from_relative(files.root, x) for x in argv]
	fp = pd.connections_index_route
	cl = fp.fs_load().decode('utf-8').split('\n')

	if position is None:
		cl.extend(map(str, targets))
	else:
		cl[(position-1):position] = map(str, targets)

	# Write unique connections; first entry wins placement.
	written = set()
	f = (lambda x: written.add(x) or x)
	fp.fs_store('\n'.join(f(x) for x in cl if x not in written and x.strip()).encode('utf-8'))

	return 0

def disconnect(pd:root.Product, contexts=None, argv=()):
	"""
	# Remove connections from product index.
	"""

	fp = pd.connections_index_route
	cl = fp.fs_load().decode('utf-8').split('\n')

	# Write unique connections; first entry wins placement.
	written = set(str(files.Path.from_relative(files.root, x)) for x in argv)
	f = (lambda x: written.add(x) or x)
	fp.fs_store('\n'.join(f(x) for x in cl if x not in written and x.strip()).encode('utf-8'))

	return 0

def build(pd:root.Product, intention='optimal', contexts=None, argv=(), lanes=4):
	from fault.time.sysclock import now

	connections = []
	symbol_index = 0
	update_index = True

	for i, a in enumerate(argv):
		if a[:2] == '-C':
			connections.append(a[2:])
		elif a == '-u':
			update_index = False
		elif a == '-U':
			update_index = True
		elif a[:2] == '-L':
			lanes = int(a[2:] or '4') # Requires decimals.
		else:
			symbol_index = i
			break
	symbols = argv[symbol_index:]

	# Default to index; -u to suppress.
	if update_index:
		index(pd)

	# Project Context
	ctx = root.Context()
	ctx.connect(pd.route)
	ctx.load()

	from fault.transcript import execution
	from fault.transcript import terminal
	from fault.transcript import fatetheme, proctheme
	from fault.transcript import integration

	# Allocate and configure control and monitors.
	control = terminal.setup()
	control.configure(lanes+1)
	monitors, summary = terminal.aggregate(control, proctheme, lanes, width=160)

	status = (control, monitors, summary)
	build_reporter = integration.emitter(integration.factor_report, sys.stdout.write)
	build_traps = execution.Traps.construct(eox=integration.select_failures, eop=build_reporter)
	with files.Path.fs_tmpdir() as cache:
		cd = (cache / 'build-cache').fs_mkdir()
		operations.build(build_traps, ctx, status, pd,
			contexts.fs_iterfiles(type='directory'), intention, cd,
			argv=symbols
		)

	return 0

def test(pd:root.Product, intention='optimal', contexts=None, argv=(), lanes=8):
	from fault.transcript import terminal
	from fault.transcript import fatetheme

	# Project Context
	ctx = root.Context()
	ctx.connect(pd.route)
	ctx.load()

	control = terminal.setup()
	control.configure(lanes+1)
	monitors, summary = terminal.aggregate(control, fatetheme, lanes, width=160)
	status = (control, monitors, summary)
	operations.test(ctx, status, pd, intention)
	return 0

def unspecified(pd:root.Product, contexts=None, argv=()):
	return 254

# Commands expecting a construction context set.
context_commands = {
	'integrate',
	'build',
	'test',
	'continue',
	'unspecified',
}

def resolve(override:str=None):
	from ..context import select

	for pair in select(local=override):
		if pair[1].fs_type() == 'directory':
			return pair
	else:
		return None

def integrate(pd:root.Product, intention='optimal', contexts=None, argv=(), lanes=4):
	"""
	# Complete build connecting requirements and updating indexes.
	"""
	from fault.time.sysclock import now, elapsed
	os.environ['FRAMECHANNEL'] = 'integrate'

	connections = []
	symbol_index = 0
	update_index = True

	for i, a in enumerate(argv):
		if a[:2] == '-C':
			connections.append(a[2:])
		elif a == '-u':
			update_index = False
		elif a == '-U':
			update_index = True
		elif a[:2] == '-L':
			lanes = int(a[2:] or '4') # Requires decimals.
		else:
			symbol_index = i
			break
	symbols = argv[symbol_index:]

	# Default to index and identify; -u to suppress.
	if update_index:
		index(pd)

	# Project Context
	ctx = root.Context()
	ctx.connect(pd.route)
	ctx.load()

	from fault.transcript import integration
	from fault.transcript import execution
	from fault.transcript import terminal
	from fault.transcript import fatetheme, proctheme

	# Allocate and configure control and monitors.
	control = terminal.setup()
	control.configure(lanes+1)
	monitors, summary = terminal.aggregate(control, proctheme, lanes, width=160)

	start_time = elapsed()
	test_usage = build_usage = 0

	sys.stdout.write("[-> Product Integration %r %s (integrate)]\n" %(str(pd.route), now().select('iso'),))
	try:
		build_reporter = integration.emitter(integration.factor_report, sys.stdout.write)
		build_traps = execution.Traps.construct(eox=integration.select_failures, eop=build_reporter)
		status = (control, monitors, summary)
		with files.Path.fs_tmpdir() as cache:
			if 1:
				cd = (cache / 'build-cache').fs_mkdir()
				operations.build(build_traps, ctx, status, pd, contexts.fs_iterfiles(type='directory'), intention, cd, symbols)
				build_usage = summary.metrics.total('usage')

		control.clear()

		monitors, summary = terminal.aggregate(control, fatetheme, lanes, width=160)
		status = (control, monitors, summary)
		if 1:
			test_reporter = integration.emitter(integration.test_report, sys.stdout.write)
			test_traps = execution.Traps.construct(eop=test_reporter)
			operations.test(test_traps, ctx, status, pd, intention)
			test_usage = summary.metrics.total('usage')
	finally:
		duration = elapsed().decrease(start_time)
		summary.title("Integration")
		summary.set_field_read_type('usage', 'overall')
		metrics = summary.metrics
		metrics.clear()
		metrics.update('usage', build_usage, 1)
		metrics.update('usage', test_usage, 1)
		metrics.commit(duration.select('millisecond') / 1000)
		sys.stdout.write("[<- %s %s (integrate)]\n" %(summary.synopsis(), now().select('iso'),))

	return 0

def main(inv:process.Invocation) -> process.Exit:
	os.environ['FPI_CACHE'] = 'transient'

	try:
		pwd = os.environ['PWD']
	except KeyError:
		pwd = str(file.Path.from_cwd())

	for i, a in enumerate(inv.argv):
		if a[:1] == '-':
			continue
		elif inv.argv[i-1] in {'-D', '-X'}:
			continue

		# End of options.
		command_index = i
		command_id = inv.argv[command_index] # update, connect, build
		break
	else:
		# No command.
		command_id = 'unspecified'
		command_index = len(inv.argv)

	options = inv.argv[:command_index]
	remainder = inv.argv[command_index+1:]

	config = {}
	key = None
	for opt in options:
		if opt[:1] == '-':
			if key is not None:
				if key in config:
					config[key] += 1
				else:
					config[key] = 1

			if len(opt) == 2:
				key = opt
			else:
				config[opt[:2]] = opt[2:]
		else:
			config[key] = opt
			key = None

	cc = None
	if command_id in context_commands:
		cc = resolve(config.get('-X') or None)
		if cc is None:
			sys.stderr.write("ERROR: no context set available\n")
			return inv.exit(10)
		else:
			cc = cc[1]

	if '-D' in config:
		pd = root.Product(files.Path.from_path(config['-D']))
	else:
		pd = root.Product(files.Path.from_absolute(pwd))

	pd.load()
	os.environ['PRODUCT'] = str(pd.route)
	cmd = globals()[command_id] # No such command.

	try:
		status = (cmd(pd, contexts=cc, argv=remainder))
	except process.Exit as failure:
		raise

	return inv.exit(status)
