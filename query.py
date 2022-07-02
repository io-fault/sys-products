"""
# Product reporting functions for integration status and profile information.

# [ Engineering ]
# Current implementation is incomplete and serves only as guidance for the intention
# of the pdctl command. In addition, it is desirable that integration status be shown
# as well. Future changes will likely include keeping integration records inside the
# product index.
"""
import collections

from fault.project import system as lsf
from fault.system import files

options = ({}, {})

def sources(factor_record):
	(fpath, ftype), (syms, src) = factor_record
	for x in src:
		yield x[1]

def stats(projects):
	rows = []

	for pj in projects:
		# File size of sources in the project.
		sizes = [[y.fs_size() for y in sources(x)] for x in pj.select(lsf.types.factor)]

		# Calculate factor count, source count, and total size of the sources.
		f_count = len(sizes)
		count = sum(len(x) for x in sizes)
		size = sum(sum(x) for x in sizes)

		rows.append((pj.corpus, pj.identifier, pj.factor, f_count, count, size))

	return rows

def report(meta, log, config, fx, cc, pdr, remainder):
	"""
	# Write human readable information about the product and the identified projects.
	"""
	if pdr.fs_type() != 'directory':
		meta.warning("product path is not a directory")
		# Nothing to do. This is not an error.
		raise SystemExit(0)

	ctx = lsf.Context()
	ctx.connect(pdr)
	ctx.load()
	ctx.configure()

	records = stats(ctx.iterprojects())
	p_count = len(records)

	g = collections.defaultdict(list)
	for r in records:
		g[r[0]].append(r)
	c_count = len(g)
	log.write(f"Product directory '{pdr!s}' contains {p_count} projects across {c_count} corpora.\n\n")

	for c, r in g.items():
		cp_count = len(r)
		log.write(f"{c} {cp_count} projects\n")
		for corpus, iid, pj_factor, f_count, src_count, src_size in r:
			fmt = f"  {pj_factor} {f_count} factors {src_count} sources {src_size} bytes {iid}\n"
			log.write(fmt)
