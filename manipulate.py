"""
# Product index delta implementation.

# Functions for manipulating the product index used to define the set of factors.
"""
from fault.project import system as lsf
from fault.system import files

options = (
	{
		# Defaults to never update.
		'-u': ('field-replace', 'missing', 'update-product-index'),
		'-U': ('field-replace', 'always', 'update-product-index'),
		'--void': ('field-replace', True, 'remove-product-index'),
	},
	{
		'-i': ('sequence-append', 'interpreted-connections'),
		'-x': ('sequence-append', 'interpreted-disconnections'),
		'-I': ('sequence-append', 'direct-connections'),
		'-X': ('sequence-append', 'direct-disconnections'),
	}
)

def index(pd:lsf.Product, statement):
	"""
	# Update the index according the requested &statement.
	"""

	if statement in {'never', None}:
		return False
	elif statement == 'always':
		# Rebuild
		pd.clear()
	elif statement == 'missing' and pd.cache.fs_type() == 'void':
		pass
	else:
		return False

	pd.update()
	pd.store()
	return True

def connecting(config):
	ci = []
	cx = []

	ci.extend(str(files.Path.from_path(x)) for x in config.get('interpreted-connections', ()))
	cx.extend(str(files.Path.from_path(x)) for x in config.get('interpreted-disconnections', ()))

	ci.extend(config.get('direct-connections', ()))
	cx.extend(config.get('direct-disconnections', ()))

	return ci, cx

def reconnect(pd:lsf.Product, insertions, deletions):
	fp = pd.connections_index_route
	cl = []
	try:
		cl.extend(fp.fs_load().decode('utf-8').split('\n'))
	except FileNotFoundError:
		pass

	cl.extend(insertions)
	written = set(deletions)

	f = (lambda x: written.add(x) or x)
	fp.fs_store('\n'.join(f(x) for x in cl if x not in written and x.strip()).encode('utf-8'))

# Rebuild project index from directory structure.
def delta(meta, log, config, fx, cc, pdr:files.Path, remainder):
	"""
	# Create or update the project index by scanning the filesystem.
	"""
	ops = 0
	pd = lsf.Product(pdr)

	if index(pd, config.get('update-product-index', None)):
		ops += 1
		meta.notice("updated project index using the product directory")

	ci, cx = connecting(config)
	if ci or cx:
		ops += 1
		reconnect(pd, ci, cx)

	if config.get('remove-product-index', False):
		ops += 1
		if pd.cache.fs_type() != 'void':
			pd.cache.fs_void()
			meta.notice("product index destroyed")
		else:
			meta.notice("product index does not exist")

	if ops == 0:
		meta.notice("no product index manipulations were performed")

	return pd
