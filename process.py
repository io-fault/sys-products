"""
# Product builds and tests for system integration.
"""
import os
from collections.abc import Iterable, Sequence

from fault.context import tools
from fault.system import files

from fault.time.sysclock import now, elapsed

from fault.transcript import terminal
from fault.transcript import fatetheme, proctheme
from fault.transcript import execution

from fault.system.execution import KInvocation
from fault.project import graph
from fault.project import system as lsf

from ..root import query

options = (
	{
		# Defaults to update when missing.
		'-u': ('field-replace', 'never', 'update-product-index'),
		'-U': ('field-replace', 'always', 'update-product-index'),

		# Integration operation controls. Disable testing/processing.
		'-t': ('field-replace', True, 'disable-functionality-tests'),
		'-T': ('field-replace', False, 'disable-functionality-tests'),
		'-b': ('field-replace', True, 'disable-factor-processing'),
		'-B': ('field-replace', False, 'disable-factor-processing'),
	},
	{}
)

def plan_build(command,
		ccontext:files.Path,
		intentions:Sequence[str],
		cache:files.Path,
		argv,
		pcontext:lsf.Context,
		identifier,
		executable=None,
		cache_type='transient',
	):
	"""
	# Create an invocation for processing &pj with &ccontext.
	"""

	pj = pcontext.project(identifier)
	project = pj.factor
	if executable is not None:
		env = dict()
		exepath = executable
		xargv = [executable]
	else:
		env, exepath, xargv = query.dispatch('factors-cc')

	pj_fp = str(project)
	ki = KInvocation(xargv[0], xargv + [
		str(ccontext), cache_type, str(cache),
		':'.join(intentions),
		str(pj.product.route),
		pj_fp,
	] + argv)

	# Factor Processing Instructions
	yield (pj_fp, (), pj_fp, ki)

def plan_test(intention:str, argv, pcontext:lsf.Context, identifier):
	"""
	# Create an invocation for processing the project from &pcontext selected using &identifier.
	"""

	pj = pcontext.project(identifier)
	project = pj.factor

	exeenv, exepath, xargv = query.dispatch('python')
	xargv.append('-d')

	for (fp, ft), fd in pj.select(lsf.types.factor@'test'):
		if not fp.identifier.startswith('test_'):
			continue

		pj_fp = str(project)
		fpath = str(fp)
		cmd = xargv + [
			'fault.test.bin.coherence',
			pj_fp, fpath
		]
		env = dict(os.environ)
		env.update(exeenv)
		env['F_PROJECT'] = str(project)
		ki = KInvocation(str(exepath), cmd, environ=env)

		# Test Fates
		yield (pj_fp, (str(fp),), '/'.join((pj_fp, str(fp))), ki)

def iterconstructs(factors:lsf.Context):
	"""
	# Iterator producing &lsf.Project instances in dependency order.
	"""
	q = graph.Queue()
	q.extend(factors)

	while not q.terminal():
		pj, = q.take(1)
		yield factors.project(pj)
		q.finish(pj)

# Build the projects within the product.
def build(meta, log, factors, status, pd:lsf.Product,
	contexts:Iterable[files.Path], intention:str,
	cache:files.Path, symbols):
	"""
	# Build all projects within the product using all &contexts.
	"""
	control, monitors, summary = status

	log.xact_open(intention, "Factor Processing Instructions", {})
	try:
		for ccontext in contexts:
			ctxid = ccontext.identifier
			q = graph.Queue()
			q.extend(factors)
			local_plan = tools.partial(
				plan_build, 'integrate',
				ccontext, [intention],
				cache, symbols, factors
			)
			execution.dispatch(meta, log, local_plan, control, monitors, summary, "FPI", q, opened=True)
	finally:
		log.xact_close(intention, summary.synopsis('FPI'), {})

	return summary.profile()

def test(meta, log, factors, status, pd:lsf.Product, argv, intention:str):
	"""
	# Test all projects.
	"""
	control, monitors, summary = status

	# In project dependency order.
	q = graph.Queue()
	q.extend(factors)
	local_plan = tools.partial(plan_test, intention, argv, factors)

	log.xact_open(intention, "Testing %s integration." %(intention,), {})
	try:
		execution.dispatch(meta, log, local_plan, control, monitors, summary, "Fates", q, opened=True)
	finally:
		log.xact_close(intention, summary.synopsis('Fates'), {})

	return summary.profile()

def integrate(meta, log, config, fx, cc, pdr:files.Path, argv, intention='optimal'):
	"""
	# Complete build connecting requirements and updating indexes.
	"""
	from fault.transcript.metrics import Procedure
	zero = Procedure.create()
	os.environ['PRODUCT'] = str(pdr)
	os.environ['INTENTION'] = intention
	os.environ['F_PRODUCT'] = str(cc)
	os.environ['F_EXECUTION'] = str(fx)
	os.environ['FRAMECHANNEL'] = 'integrate'

	xbuild = config.get('disable-factor-processing', False) == False
	xtest = config.get('disable-functionality-tests', False) == False

	lanes = int(config['processing-lanes'])
	connections = []
	metrics = []
	symbol_index = 0
	symbols = argv

	idx_update = config.get('update-product-index', 'missing')
	if idx_update != 'never':
		# Default for integrate is to update if it is missing.
		# This is different from &manipulate.delta's default.
		from . import manipulate
		if manipulate.index(lsf.Product(pdr), idx_update):
			meta.notice(None, "updated (" + str(idx_update) + ") project index using the directory")

	# Project Context
	factors = lsf.Context()
	pd = factors.connect(pdr)
	factors.load()
	factors.configure()

	# Allocate and configure control and monitors.
	control = terminal.setup()
	control.configure(lanes+1)

	from fault.system.query import hostname
	ts = now().select('iso')
	host = hostname()

	log.xact_open('integrate',
		"Product Integration (%s) of %r on %s at %s" %(
			intention, str(pd.route), host, ts,
		),
		{
			'timestamp': [ts],
			'hostname': [host],
			'product': [str(pd.route)],
		}
	)

	start_time = elapsed()
	profiles = []
	try:
		if xbuild:
			monitors, summary = terminal.aggregate(control, proctheme, lanes, width=160)
			status = (control, monitors, summary)

			with files.Path.fs_tmpdir() as cache:
				cd = (cache / 'build-cache').fs_mkdir()
				try:
					profiles.append(build(
						meta, log, factors, status, pd,
						[cc], intention, cd, symbols
					))
				finally:
					control.clear()

		if xtest:
			monitors, summary = terminal.aggregate(control, fatetheme, lanes, width=160)
			status = (control, monitors, summary)

			try:
				profiles.append(test(meta, log, factors, status, pd, [], intention))
			finally:
				control.clear()
				control.device.drain()
	finally:
		stop_time = elapsed()
		duration = stop_time.decrease(start_time)
		ru = zero.usage
		for start, stop, m in profiles:
			ru += m.usage
		totals = Procedure(work=zero.work, msg=zero.msg, usage=ru)
		summary.reset(start_time, zero)
		summary.update(start_time, zero)
		summary.update(stop_time, totals)
		log.emit(summary.frame('<-', "Integration", log.channel))
