package org.elasticsearch.index.conflict;

import java.util.ArrayList;
import java.util.List;

/**
 * A composite class for a chain of processors on the failed event
 */
public class FailedEventProcessors implements FailedEventProcessor {
    List<FailedEventProcessor> processorChain = new ArrayList<>();

  public FailedEventProcessors() {
  }

  public FailedEventProcessors addProcessor(FailedEventProcessor fp) {
    processorChain.add(fp);
    return this;
  }

  @Override
  public Event process(FailedEvent fe) {
    if (processorChain.isEmpty()) {
      return fe.getOriginalEvent();
    }

    Event e = null;
    for (FailedEventProcessor fp: processorChain) {
      e = fp.process(fe);
      fe = new FailedEvent(e, fe.getFailureReason());
    }
    return e;
  }
}
