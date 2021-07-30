"""
# Product builds and tests for the local system.
"""
import sys
import os
import typing

from fault.context import tools
from fault.system import files

from fault.transcript import execution
from fault.system.execution import KInvocation
from fault.project import graph
from fault.project import system as lsf

from ..root import query

def update(pd:lsf.Product):
	pd.clear()
	pd.update()
	pd.store()

def plan_build(command,
		ccontext:files.Path, intentions:typing.Sequence[str],
		cache:files.Path, argv, pcontext:lsf.Context, identifier
	):
	"""
	# Create an invocation for processing &pj with &ccontext.
	"""

	pj = pcontext.project(identifier)
	project = pj.factor
	env, exepath, xargs = query.dispatch('factors-cc')

	dims = (str(project),)
	xid = '/'.join(dims)

	ki = KInvocation(xargs[0], xargs + [
		str(ccontext),
		'transient', str(cache),
		':'.join(intentions),
		str(pj.product.route), str(project)
	] + argv)

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
		yield ('Fates', dims, xid, None, ki)

# Render FPI references.
def iterconstructs(pd:lsf.Product, contexts:typing.Iterable[files.Path], intention:str, cache:files.Path, argv=None):
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

# Build the product or a project set.
def build(traps, ctx, status, pd:lsf.Product,
	contexts:typing.Iterable[files.Path], intention:str,
	cache:files.Path, argv=None, rebuild=False):
	"""
	# Build all projects within product using the &contexts.
	"""
	control, monitors, summary = status
	metrics = []

	sys.stdout.write("[-> Processing factors for %r intent. (integrate/build)]\n" %(intention,))
	try:
		for ccontext in contexts:
			ctxid = ccontext.identifier
			constants = ('integrate', 'build', ctxid,)
			sys.stdout.write("[-> Processing factors with %r context. (integrate/build/%s)]\n" %(ctxid, ctxid))
			try:
				q = graph.Queue()
				q.extend(ctx)
				local_plan = tools.partial(plan_build, 'integrate', ccontext, [intention], cache, argv, ctx)
				execution.dispatch(traps, local_plan, control, monitors, summary, "FPI", constants, q)
			finally:
				summary.set_field_read_type('usage', 'overall')
				sys.stdout.write("[<- %s (integrate/build/%s)]\n" %(summary.synopsis(), ctxid))

			metrics.append(summary.metrics.export())
	finally:
		summary.metrics.clear()
		for m in metrics:
			summary.metrics.apply(m)
		summary.set_field_read_type('usage', 'overall')

		sys.stdout.write("[<- %s (integrate/build)]\n" %(summary.synopsis(),))

def test(traps, ctx, status, pd:lsf.Product, intention:str, argv=None):
	"""
	# Test all projects.
	"""
	control, monitors, summary = status

	q = graph.Queue()
	q.extend(ctx)
	local_plan = tools.partial(plan_test, intention, argv, ctx)

	sys.stdout.write("[-> Testing %r build. (integrate/test)]\n" %(intention,))
	try:
		constants = ('integrate', 'test', intention,)
		execution.dispatch(traps, local_plan, control, monitors, summary, "Fates", constants, q)
	finally:
		summary.set_field_read_type('usage', 'overall')
		sys.stdout.write("[<- %s (integrate/test)]\n" %(summary.synopsis(),))
