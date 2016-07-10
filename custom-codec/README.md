This example shows how to define custom partitions and a custom `StreamCodec` to customize
the set of tuples that reach each partition.

There are two operators:
- `RandomGenerator` which generates lines and emits them on the output port. 
   The lines have the form "i^j^hello" where `i` is the sequence 1, 2, 3, ....
   and `j` cycles through the range 1..5 .

- `TestPartition` which is a partitioned operator that logs input tuples.

The application uses a StreamCodec called `MyCodec` to tag each tuple with a
partition tag based on the hash code of j.

`TestPartition` is partitioned into 3 replicas using `StatelessPartitioner` from
the Malhar library.
