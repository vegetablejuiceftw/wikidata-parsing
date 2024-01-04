Here is an example script to read the MessagePack shards in a MultiProcess Pool.

The main dependency are `msgpack` and `tqdm`. 

To setup the dependencies and virtualenv with poetry run:

    poetry install --no-root

After this you can activae the virtual env n your console with

    poetry shell

And then run 

    python shard_reader.py


Make sure to also edit `SHARD_PATH = "../data/shard-16-*.gz"` to point to the correct files.
