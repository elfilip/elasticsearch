package org.elasticsearch.index.conflict;

import java.util.ArrayList;

public class MappingExceptionProcessor implements FailedEventProcessor {

  private static final ArrayList<MappingConflictResolver> mappingConflictResolvers = MappingConflictUtils.initMappingConflictResolvers();

  @Override
  public Event process(FailedEvent fe) {
    if (fe.getFailureReason().contains("MapperParsingException")) {
      return filterMappingErrors(fe.getOriginalEvent(), fe.getFailureReason());
    }

    return fe.getOriginalEvent();
  }

  private Event filterMappingErrors(Event originalEvent, String failureReason) {
    Event resolvedEvent = null;
    for (MappingConflictResolver resolver : mappingConflictResolvers) {
      resolvedEvent = resolver.resolveMappingConflict(originalEvent, failureReason);
      if (resolvedEvent != null) {
        return resolvedEvent;
      }
    }

    if(resolvedEvent == null) {
      MappingConflictUtils.removeAllButSyslogFields(originalEvent);
    }

    return originalEvent;
  }
}
