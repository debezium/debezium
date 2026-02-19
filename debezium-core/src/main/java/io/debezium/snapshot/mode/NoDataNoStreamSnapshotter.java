package io.debezium.snapshot.mode;

public class NoDataNoStreamSnapshotter extends NoDataSnapshotter {
    @Override
    public String name() {
        return "no_data_no_stream";
    }

    @Override
    public boolean shouldStream() {
        return false;
    }
}
