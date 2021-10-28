"""
# Product configuration and integration system command.
"""
import sys
import os
import importlib

from fault.vector import recognition
from fault.system import files
from fault.system import process

from .. import __name__ as project_package_name
from .. import context

restricted = {
	'-c': ('field-replace', ".build-cache", 'persistent-cache'),
}

required = {
	'-x': ('field-replace', 'construction-context'),
	'-X': ('field-replace', 'cc-executable'),

	'-D': ('field-replace', 'product-directory'),
	'-L': ('field-replace', 'processing-lanes'),
	'-C': ('field-replace', 'persistent-cache'),
}

command_index = {
	# Process factors with respect to the product index.
	'integrate': ('.process', 'options', 'integrate'),

	# Manipulate product index and connections.
	'delta': ('.manipulate', 'options', 'delta'),

	# Show product status.
	'status': ('.query', 'options', 'report')
}

def configure(restricted, required, argv):
	"""
	# Root option parser.
	"""
	config = {
		'intentions': set(),
		'processing-lanes': '4',
		'construction-context': None,
		'persistent-cache': None,
		'product-directory': None,
		'default-product': None,

		'interpreted-connections': [],
		'interpreted-disconnections': [],
		'direct-connections': [],
		'direct-disconnections': [],
	}

	oeg = recognition.legacy(restricted, required, argv)
	remainder = recognition.merge(config, oeg)

	return config, remainder

def main(inv:process.Invocation) -> process.Exit:
	try:
		pwd = os.environ['PWD']
	except KeyError:
		pwd = str(file.Path.from_cwd())

	config, remainder = configure(restricted, required, inv.argv)
	if not remainder:
		command_id = 'status'

	if remainder:
		command_id = remainder[0]
	else:
		command_id = 'help'

	if command_id == 'help':
		sys.stderr.write("pdctl [-D product-directory] (integrate | status | delta) [system-symbols]\n")
		return inv.exit(63)
	elif command_id not in command_index:
		sys.stderr.write("ERROR: unknown command '%s'." %(command_id,))
		return inv.exit(2)

	module_name, optset, operation = command_index[command_id]
	module = importlib.import_module(module_name, project_package_name)
	oprestricted, oprequired = getattr(module, optset)

	cmd_oeg = recognition.legacy(oprestricted, oprequired, remainder[1:])
	cmd_remainder = recognition.merge(config, cmd_oeg)

	origin, cc = context.resolve(config['construction-context'])

	if config['product-directory']:
		pdr = files.Path.from_path(config['product-directory'])
		config['default-product'] = False
	else:
		pdr = files.Path.from_absolute(pwd)
		config['default-product'] = True

	pd_oper = getattr(module, operation)
	pd_oper(sys.stdout.write, config, cc, pdr, cmd_remainder)
	return inv.exit(0)
