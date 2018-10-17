package org.elasticsearch.index.conflict;

import java.io.Serializable;

/**
 * Wrapper Class that encapsulates an event which failed in our pipeline.
 * The failure can be at any stage like indexing, parsing, mapping
 */
public class FailedEvent implements Serializable {
  private static final long serialVersionUID = 1L;

  private final Event event;
  private final String failureReason;

  public FailedEvent(Event event, String failureReason) {
    this.event = event;
    this.failureReason = (failureReason == null ? "No failure reason": failureReason);
  }

  public String getFailureReason() {
    return this.failureReason;
  }

  public Event getOriginalEvent() {
    return this.event;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof FailedEvent)) return false;

    FailedEvent that = (FailedEvent) o;

    if (!event.equals(that.event)) return false;
    if (!failureReason.equals(that.failureReason)) return false;

    return true;
  }

  @Override
  public int hashCode() {
    int result = event.hashCode();
    result = 31 * result + failureReason.hashCode();
    return result;
  }

  @Override
  public String toString() {
    return "FailedEvent@" + Integer.toHexString(hashCode())
        + "[event=" + event
        + ", failureReason=" + failureReason
        + ']';
  }
}
