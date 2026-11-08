"""Partition the standard unittest discovery suite across CI jobs."""
import argparse
import unittest


def iter_tests(suite):
    for item in suite:
        if isinstance(item, unittest.TestSuite):
            yield from iter_tests(item)
        else:
            yield item


def select_tests(tests, index, count):
    return tests[index::count]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--shard-index', type=int, required=True)
    parser.add_argument('--shard-count', type=int, required=True)
    args = parser.parse_args()
    if args.shard_count < 1 or not 0 <= args.shard_index < args.shard_count:
        parser.error('shard index must be within the positive shard count')
    tests = list(iter_tests(unittest.defaultTestLoader.discover('test', pattern='*test.py')))
    selected = select_tests(tests, args.shard_index, args.shard_count)
    if not selected:
        parser.error('no tests assigned; check discovery and the shard configuration')
    print(f'Shard {args.shard_index + 1}/{args.shard_count}: '
          f'{len(selected)}/{len(tests)} tests', flush=True)
    result = unittest.TextTestRunner(verbosity=2).run(unittest.TestSuite(selected))
    return 0 if result.wasSuccessful() else 1


if __name__ == '__main__':
    raise SystemExit(main())
