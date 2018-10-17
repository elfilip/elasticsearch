package org.elasticsearch.index.conflict;

import java.util.ArrayList;

public class MappingExceptionProcessor implements FailedEventProcessor {

  private static final ArrayList<MappingConflictResolver> mappingConflictResolvers = MappingConflictUtils.initMappingConflictResolvers();

  @Override
  public Event process(FailedEvent fe) {
    if (fe.getFailureReason().contains("MapperParsingException")) {
     /* if(Flag.LogMappingConflictException.isset()) {
        // Log mapping conflict exceptions for offline analysis.
        //LOG.info("Received the following MapperParsingException: "+fe.getFailureReason()+". Original event received was: "+fe.getOriginalEvent());
      }*/

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
      // none of the resolvers were able to fix the event
    /*  if(Flag.LogUnknownMappingConflictException.isset()) {
        //LOG.warn("Unable to resolve the mapping conflict for the following event: " + originalEvent + ". Failure reason for" +
        //    "mapping conflict was: " + failureReason);
      }*/
      MappingConflictUtils.removeAllButSyslogFields(originalEvent);
    }

    return originalEvent;
  }
}
