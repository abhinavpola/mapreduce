# MapReduce

Wrote this scaffolding to practice MapReduce problems (simulated via processes).

## Usage

`python mapreduce.py`

If you want to try it on different problems, you can modify:

- `dataset()` -> input data.
- `split()` -> hostnames, paths, offsets, etc.
- `map_func()`
- `reduce_func()`

## Some not so idiomatic parts

- The obvious non-distributed nature of this.
- Queues, poison pills, the output file, etc.

## TODO

- Make it cleaner and more extensible. split, map, shuffle, reduce, combine, should be fill-in-the-blank boilerplate functions.
- Maybe add more datasets and problems to try.
