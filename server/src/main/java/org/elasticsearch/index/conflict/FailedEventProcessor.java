package org.elasticsearch.index.conflict;

public interface FailedEventProcessor {
    public Event process(FailedEvent fe);
}
