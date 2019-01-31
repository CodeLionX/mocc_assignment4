#!/usr/bin/env python3

import itertools
import collections
import sys

import matplotlib.pyplot as plt


def load_clusters(filename, skip_header_line=True):
	funcs = [int] + [float, float]
	cast = lambda l: tuple(f(e) for f, e in zip(funcs, l))

	input_file = open(filename, encoding="utf-8")

	if skip_header_line:
		next(input_file)
	items = (cast(line.strip().split(",")) for line in input_file)
	clusters = collections.defaultdict(list)
	for key, lon, lat in items:
		clusters[key].append((lon, lat))

	return clusters


def scatter(ax, cells, c, s):
	xs, ys = itertools.tee(cells)
	xs, ys = [x for x, _ in xs], [y for _, y in ys]
	ax.scatter(xs, ys, c=c, s=s*0.1)

colors = ['#E95420', '#EB6536', '#ED764D', '#F08763', '#772953', '#843E64', '#925375', '#9F6986', "green", "blue", "red", "yellow", "orange", "black", "cyan", "lightblue"]

def plot(clusters):
	fig, ax = plt.subplots()

	for idx, key in enumerate(clusters):
		scatter(ax, clusters[key], colors[idx % len(colors)], 5)

	plt.show()

if __name__ == "__main__":
	if len(sys.argv) < 2:
		sys.stderr.write("Usage: %s <clusters-csv>\n" % sys.argv[0])
		sys.exit(1)
	clusters = load_clusters(sys.argv[-1])
	plot(clusters)
