"""
# Construction Context resolution for &..products.
"""
import os
import typing

from fault.system import files

def select(local:str=None,
		environment='CONTEXTSET',
		fault='FAULT',
		home='HOME', user='.cc'
	) -> typing.Tuple[str, files.Path]:
	"""
	# Generate the possible locations of a usable context set.
	# Usually, the first existing path should be used.

	# Some entries will only be present when a required environment variable
	# is present and not an empty string. Existence must be checked by caller:

	#!syntax/python
		[x for x in context.select(...) if x[1].fs_type() == 'directory']

	# The first element of the tuples is a string describing the origin of the path.
	"""

	if local:
		# A, normally, user provided path.
		yield ('local', files.Path.from_path(local))

	if os.environ.get(environment, None):
		yield ('environment', files.Path.from_absolute(os.environ[environment]))

	yield ('user', files.Path.from_absolute(os.environ[home]) / user) # No HOME?

	if os.environ.get(fault, None):
		yield ('fault', files.Path.from_absolute(os.environ[fault]) / 'cc')
