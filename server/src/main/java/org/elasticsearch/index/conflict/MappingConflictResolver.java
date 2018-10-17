package org.elasticsearch.index.conflict;

public interface MappingConflictResolver {

  /**
   * An object that attempts to resolve a mapping conflict as best as it can by removing or munging fields
   * as necessary and adding appropriate notifications to the failed event.
   *
   * @param originalEvent
   * @param mappingExceptionMessage
   * @return An event that has been munged in order to resolve the mapping conflict, OR null if the mapping conflict could not be resolved.
   */
  public Event resolveMappingConflict(Event originalEvent, String mappingExceptionMessage);
}
