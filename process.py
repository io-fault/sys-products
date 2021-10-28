"""
# Product builds and tests for the local system.
"""
import os
from collections.abc import Iterable, Sequence

from fault.context import tools
from fault.system import files

from fault.time.sysclock import now, elapsed

from fault.transcript import integration
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
		xargs = [executable]
	else:
		env, exepath, xargs = query.dispatch('factors-cc')

	dims = (str(project),)
	xid = '/'.join(dims)

	ki = KInvocation(xargs[0], xargs + [
		str(ccontext), cache_type, str(cache),
		':'.join(intentions),
		str(pj.product.route),
		str(project)
	] + argv)

	# Factor Processing Instructions
	yield ('FPI', dims, xid, None, ki)

def plan_test(intention:str, argv, pcontext:lsf.Context, identifier):
	"""
	# Create an invocation for processing the project from &pcontext selected using &identifier.
	"""

	pj = pcontext.project(identifier)
	project = pj.factor

	exeenv, exepath, xargs = query.dispatch('test-python-module')

	for (fp, ft), fd in pj.select(lsf.types.factor@'test'):
		if not fp.identifier.startswith('test_'):
			continue

		cmd = xargs + [str(project), str(fp)]
		env = dict(os.environ)
		env.update(exeenv)
		env['PROJECT'] = str(project)
		ki = KInvocation(str(exepath), cmd, environ=env)
		dims = (str(project), str(fp))
		xid = '/'.join(dims)

		# Test Fates
		yield ('Fates', dims, xid, None, ki)

# Render FPI references.
def iterconstructs(pd:lsf.Product,
	contexts:Iterable[files.Path], intention:str,
	cache:files.Path, argv=None):
	ctx = lsf.Context()
	ctx.connect(pd.route)
	ctx.load()

	for ccontext in contexts:
		q = graph.Queue()
		q.extend(ctx)

		while not q.terminal():
			pj = ctx.project(list(q.take(1))[0])
			yield plan(pj, ccontext, intention, cache, argv)
			q.finish(pj.identifier)

# Build the projects within the product.
def build(log, traps, ctx, status, pd:lsf.Product,
	contexts:Iterable[files.Path], intention:str,
	cache:files.Path, symbols):
	"""
	# Build all projects within product using the &contexts.
	"""
	control, monitors, summary = status
	metrics = []

	log("[-> Processing factors for %r intent. (integrate/build)]\n" %(intention,))
	try:
		for ccontext in contexts:
			ctxid = ccontext.identifier
			constants = ('integrate', 'build', ctxid,)
			log("[-> Processing factors with %r context. (integrate/build/%s)]\n" %(ctxid, ctxid))
			try:
				q = graph.Queue()
				q.extend(ctx)
				local_plan = tools.partial(
					plan_build, 'integrate',
					ccontext, [intention],
					cache, symbols, ctx
				)
				execution.dispatch(traps, local_plan, control, monitors, summary, "FPI", constants, q)
			finally:
				summary.set_field_read_type('usage', 'overall')
				log("[<- %s (integrate/build/%s)]\n" %(summary.synopsis(), ctxid))

			metrics.append(summary.metrics.export())
	finally:
		summary.metrics.clear()
		for m in metrics:
			summary.metrics.apply(m)
		summary.set_field_read_type('usage', 'overall')

		log("[<- %s (integrate/build)]\n" %(summary.synopsis(),))

def test(log, traps, ctx, status, pd:lsf.Product, argv, intention:str):
	"""
	# Test all projects.
	"""
	control, monitors, summary = status

	# In project dependency order.
	q = graph.Queue()
	q.extend(ctx)
	local_plan = tools.partial(plan_test, intention, argv, ctx)

	log("[-> Testing %r build. (integrate/test)]\n" %(intention,))
	try:
		constants = ('integrate', 'test', intention,)
		execution.dispatch(traps, local_plan, control, monitors, summary, "Fates", constants, q)
	finally:
		summary.set_field_read_type('usage', 'overall')
		log("[<- %s (integrate/test)]\n" %(summary.synopsis(),))

def integrate(log, config, cc, pdr:files.Path, argv, intention='optimal'):
	"""
	# Complete build connecting requirements and updating indexes.
	"""
	os.environ['PRODUCT'] = str(pdr)
	os.environ['F_PRODUCT'] = str(cc)
	os.environ['FRAMECHANNEL'] = 'integrate'

	built = tested = False
	lanes = int(config['processing-lanes'])
	connections = []
	symbol_index = 0
	symbols = argv

	idx_update = config.get('update-product-index', 'missing')
	if idx_update != 'never':
		# Default for integrate is to update if it is missing.
		# This is different from &manipulate.delta's default.
		from . import manipulate
		if manipulate.index(lsf.Product(pdr), idx_update):
			log(f"[!# NOTICE: updated ({idx_update}) project index using the product directory.]\n")

	# Project Context
	ctx = lsf.Context()
	pd = ctx.connect(pdr)
	ctx.load()
	ctx.configure()

	# Allocate and configure control and monitors.
	control = terminal.setup()
	control.configure(lanes+1)
	monitors, summary = terminal.aggregate(control, proctheme, lanes, width=160)

	start_time = elapsed()
	test_usage = build_usage = 0

	log(
		"[-> Product Integration %r %s (integrate)]\n" %(
			str(pd.route),
			now().select('iso'),
		)
	)

	try:
		if config.get('disable-factor-processing', False) == False:
			built = True
			build_reporter = integration.emitter(integration.factor_report, log)
			build_traps = execution.Traps.construct(
				eox=integration.select_failures, eop=build_reporter
			)
			status = (control, monitors, summary)
			with files.Path.fs_tmpdir() as cache:
				cd = (cache / 'build-cache').fs_mkdir()
				build(
					log, build_traps, ctx, status, pd,
					[cc],
					intention, cd,
					symbols
				)
				build_usage = summary.metrics.total('usage')

		if config.get('disable-functionality-tests', False) == False:
			tested = True

			if built:
				control.clear()

			monitors, summary = terminal.aggregate(control, fatetheme, lanes, width=160)
			status = (control, monitors, summary)

			test_reporter = integration.emitter(integration.test_report, log)
			test_traps = execution.Traps.construct(eop=test_reporter)
			test(log, test_traps, ctx, status, pd, [], intention)
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
		log("[<- %s %s (integrate)]\n" %(summary.synopsis(), now().select('iso'),))
