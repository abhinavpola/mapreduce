# Mapreduce simulation using multiprocessing
# Doing character count as an example but can be generalized

import random
import math
from multiprocessing import Manager, Process
from concurrent.futures import as_completed, ProcessPoolExecutor
from collections import Counter, defaultdict

letters = "abcdefghijklmnopqrstuvwxyz"


def dataset():
    random.seed(42)
    result = ""
    # 1000 lines
    for _ in range(1000):
        # newline of 1000 random letters
        result += "".join(random.choices(letters, k=1000))
        result += "\n"
    ground_truth = Counter(result)
    return result, ground_truth


def split(dataset_length, num_splits):
    # returns [start, end]
    splits = []
    chars_per_split = math.ceil(dataset_length / num_splits)
    for i in range(num_splits):
        splits.append((i * chars_per_split, (i + 1) * chars_per_split))
    return splits


def fetch_data(split):
    # simulating a network call
    data, _ = dataset()
    return data[split[0] : split[1]]


def map_func(
    split, worker_id, buffer_size, num_reducers, combine_func=None, combine_final=False
):
    def merge(buffer1, buffer2):
        # print(f"Merging two buffers in worker {worker_id}")
        buffer1.extend(buffer2)
        buffer1.sort(
            key=lambda x: x[0]
        )  # O(n log n) since we didn't implement merge sort, but this would typically be O(n)

    # pull the dataset
    data = fetch_data(split)
    intermediate_results = []
    buffer = []

    # Create output file name
    output_filename = f"output_{worker_id}.txt"

    # process the split
    i = 0
    while i < len(data):
        if len(buffer) == buffer_size:
            # spill to disk immediately when buffer is full
            buffer.sort(key=lambda x: x[0])
            if combine_func:
                buffer = combine_func(buffer)

            # Merge with intermediate results first
            prev_len = len(intermediate_results)
            merge(intermediate_results, buffer)
            assert len(intermediate_results) == prev_len + len(buffer)

            # Write the current intermediate results to file
            with open(output_filename, "w") as f:
                for key, value in intermediate_results:
                    f.write(f"{key} {value}\n")

            buffer = []
        else:
            if data[i] == "\n":
                i += 1
                continue
            buffer.append((data[i], 1))  # emit(key, 1)
            i += 1

    # spill the remaining buffer
    if buffer:
        buffer.sort(key=lambda x: x[0])
        if combine_func:
            buffer = combine_func(buffer)
        prev_len = len(intermediate_results)
        merge(intermediate_results, buffer)
        assert len(intermediate_results) == prev_len + len(buffer)

        # Write the final intermediate results to file
        with open(output_filename, "w") as f:
            for key, value in intermediate_results:
                f.write(f"{key} {value}\n")

    if combine_final:
        intermediate_results = combine_func(intermediate_results)

    print(f"Mapper {worker_id} has {len(intermediate_results)} intermediate results")

    # assign to reducers, this must be reported to the master worker via metadata
    # we map reducer -> offsets in the output file where the intermediate results are written
    metadata = defaultdict(list)

    # Calculate offsets from intermediate_results
    current_key = None
    start_position = 0
    key_count = 0

    for i, (key, _) in enumerate(intermediate_results):
        if current_key is None:
            # First key
            current_key = key
            start_position = 0
            key_count = 1
        elif key != current_key:
            # Key changed, record the previous key's range
            reducer_idx = hash(current_key) % num_reducers
            metadata[reducer_idx].append((start_position, key_count))

            # Start tracking the new key
            current_key = key
            start_position = i
            key_count = 1
        else:
            key_count += 1

    # Don't forget the last key group
    if current_key is not None:
        reducer_idx = hash(current_key) % num_reducers
        metadata[reducer_idx].append((start_position, key_count))

    # fill in missing reducers
    for r in range(num_reducers):
        if r not in metadata:
            metadata[r] = [(0, 0)]

    print(f"Done mapping with mapper {worker_id}")
    return (metadata, output_filename)


def reduce_worker(reducer_idx, shuffle_dict, output_file):
    # shuffle_dict[reducer_idx].get() returns an item from a list of tuples of key-values
    collected_data = defaultdict(list)

    while True:
        # This will block until data is available
        metadata = shuffle_dict[reducer_idx].get()
        if metadata is None:
            print(f"Reducer {reducer_idx} received poison pill")
            break
        else:
            print(f"Reducer {reducer_idx} is collecting key-values")
            key, values = metadata
            print(
                f"Reducer {reducer_idx} is collecting key {key} with {len(values)} values"
            )
            collected_data[key].extend(values)

    # Now that all mappers are done, process all collected data
    print(f"Reducer {reducer_idx} processing all collected data")
    for key, values in collected_data.items():
        reduce_func(key, values, output_file)

    print(f"Done reducing with reducer {reducer_idx}")


def reduce_func(key, values, output_file):
    # supposed to write to output file, but we'll just write to a dict
    result = 0
    for value in values:  # polling for its key's values
        result += value
    if key not in output_file:
        output_file[key] = result
    else:
        output_file[key] += result
    print(f"Done reducing with key {key}")


def mapreduce(offsets, num_mappers, num_reducers):
    assert len(offsets) == num_mappers

    # shuffle dict
    with Manager() as manager:
        # initialize the shuffle dict
        shuffle_dict = manager.dict()
        output_file = manager.dict()
        for r in range(num_reducers):
            shuffle_dict[r] = manager.Queue()

        with ProcessPoolExecutor(num_mappers + num_reducers) as executor:
            # kick off mappers
            reported_metadata_futures = []
            for i, split in enumerate(offsets):
                reported_metadata_futures.append(
                    executor.submit(
                        map_func, split, i, 50000, num_reducers, None, False
                    )
                )

            assert len(reported_metadata_futures) == num_mappers

            running_reducers = []

            # kick off reducer workers to poll for their shuffled and sorted values
            for r in range(num_reducers):
                running_reducers.append(
                    executor.submit(reduce_worker, r, shuffle_dict, output_file)
                )
            assert len(running_reducers) == num_reducers

            for future in as_completed(reported_metadata_futures):
                metadata, filename = future.result()
                print(f"Mapper returned {len(metadata)} partitionings at {filename}")
                for reducer_idx, boundaries in metadata.items():
                    for start, length in boundaries:
                        with open(filename, "r") as f:
                            print(
                                f"Reducer {reducer_idx} is reading from {start} to {start + length} lines in {filename}"
                            )
                            for _ in range(start):
                                f.readline()
                            values = []
                            prev_key, value = f.readline().split()
                            values.append(int(value))
                            for _ in range(length - 1):
                                key, value = f.readline().split()
                                if key != prev_key:
                                    raise ValueError(
                                        f"Key changed from {prev_key} to {key}"
                                    )
                                values.append(int(value))
                                prev_key = key
                            print(
                                f"Reducer {reducer_idx} received {len(values)} values for key {key}"
                            )
                            shuffle_dict[reducer_idx].put((key, values))

            # Only send poison pills after all mappers are done
            print(
                "All mappers have completed. Sending termination signals to reducers..."
            )
            for r in range(num_reducers):
                shuffle_dict[r].put(
                    None
                )  # poison pill after mappers are done reporting

            # get all the reducer's results since the metadata stream is done.
            for future in running_reducers:
                future.result()
        result = dict(output_file)

    return result


if __name__ == "__main__":
    data, ground_truth = dataset()
    del ground_truth["\n"]
    print(f"Ground truth: {ground_truth}")
    output = mapreduce(split(len(data), 4), 4, 4)
    print(f"Output: {output}")
    assert len(output) == len(ground_truth)
    for key, value in output.items():
        assert value == ground_truth[key]

    print("All values match")
