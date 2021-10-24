"""
# Product index delta implementation.

# Functions for manipulating the product index used to define the set of factors.
"""
from fault.project import system as lsf
from fault.system import files

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
	cl = fp.fs_load().decode('utf-8').split('\n')

	cl.extend(insertions)
	written = set(deletions)

	f = (lambda x: written.add(x) or x)
	fp.fs_store('\n'.join(f(x) for x in cl if x not in written and x.strip()).encode('utf-8'))

# Rebuild project index from directory structure.
def delta(log, config, cc, pdr:files.Path, remainder):
	"""
	# Create or update the project index by scanning the filesystem.
	"""
	ops = 0
	pd = lsf.Product(pdr)

	if index(pd, config.get('update-product-index', None)):
		ops += 1
		log("[!# NOTICE: updated project index using the product directory.]\n")

	ci, cx = connecting(config)
	if ci or cx:
		ops += 1
		reconnect(pd, ci, cx)

	if config.get('remove-product-index', False):
		ops += 1
		if pd.cache.fs_type() != 'void':
			pd.cache.fs_void()
			log("[!# NOTICE: product index destroyed.]\n")
		else:
			log("[!# NOTICE: product index does not exist.]\n")

	if ops == 0:
		log("[!# NOTICE: no product index manipulations were performed.]\n")

	return pd