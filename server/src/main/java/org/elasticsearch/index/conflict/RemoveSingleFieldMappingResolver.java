package org.elasticsearch.index.conflict;

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class RemoveSingleFieldMappingResolver implements MappingConflictResolver {
  private final List<Pattern> patterns;
  private final String notificationMessage;
  private final boolean fieldIsFullyQualified;
  private String fieldGroupToSearch;

  /**
   * A mapping conflict resolver that looks for a single field in the elasticsearch MapperParsingException message and attempts to remove that field.
   *
   *
   * @param notificationMessage The message that is included in the notification that is attached to the event.
   *                            This message is appended with what field was removed.
   * @param regexes A list of regexes to match against the input message. Each regex must have exactly one capture group indicating what the field name is in
   *                the elasticsearch MapperParsingException message.
   */
  public RemoveSingleFieldMappingResolver(String notificationMessage, String... regexes ) {
    this.patterns = new ArrayList<>(regexes.length);
    for(String regex : regexes) {
      patterns.add(Pattern.compile(regex));
    }
    this.notificationMessage = notificationMessage;
    this.fieldIsFullyQualified = true;
  }

  /**
   * A mapping conflict resolver that searches in the given field group for the field causing the conflict. This should be used
   * when the ES exception message does not include the fully qualified field path and the field must be searched for within
   * the given field group.
   *
   * @param notificationMessage
   * @param fieldGroup
   * @param regexes
   */
  public RemoveSingleFieldMappingResolver(String notificationMessage, String fieldGroup, String... regexes ) {
    this.patterns = new ArrayList<>(regexes.length);
    for(String regex : regexes) {
      patterns.add(Pattern.compile(regex));
    }
    this.notificationMessage = notificationMessage;
    this.fieldIsFullyQualified = false;
    this.fieldGroupToSearch = fieldGroup;
  }

  /**
   * Try to resolve the mapping conflict. Return null if none of the regexes matched.
   *
   * @param originalEvent
   * @param mappingExceptionMessage
   * @return
   */
  public Event resolveMappingConflict(Event originalEvent, String mappingExceptionMessage) {
    String fieldCausingConflict = getConflictingFieldFromMessage(mappingExceptionMessage);
    return fieldCausingConflict == null ? null : resolve(originalEvent, fieldCausingConflict);
  }

  /**
   * Resolves the mapping conflict with a best effort attempt at removing the field causing the conflict.
   *
   * @param originalEvent
   * @param fieldCausingConflict
   * @return
   */
  private Event resolve(Event originalEvent, String fieldCausingConflict) {
    if(fieldCausingConflict == null) {
      return originalEvent;
    }

    if(fieldIsFullyQualified) {
      removeFullyQualifiedField(originalEvent, fieldCausingConflict);
    } else {
      if(!originalEvent.containsFieldGroup(originalEvent.getCustomerID())) {
        // we have an unqualified field from ES and the field group that we should search for the field in is missing, so we remove all fields
        // this shouldn't ever really happen but just in case we do it so that the event can still be successfully indexed without another mapping conflict
        MappingConflictUtils.removeAllButSyslogFields(originalEvent);
      } else {
          Map<String, Object> fieldGroup;
          if(fieldGroupToSearch.equals("json")){
              fieldGroup = originalEvent.getFieldGroup(originalEvent.customerID);
              if(fieldGroup == null){
                  MappingConflictUtils.removeAllButSyslogFields(originalEvent);
                  originalEvent.setNotification(NotificationKey.MappingConflict, notificationMessage + " Removed all fields.");
              }
          }else{
              fieldGroup = originalEvent.getFieldGroup(this.fieldGroupToSearch);
          }

        Object removedValue = searchAndRemoveField(originalEvent, fieldGroup, fieldCausingConflict, new LinkedList<>());
        if(removedValue != null) {
          originalEvent.setNotification(NotificationKey.MappingConflict, notificationMessage + " Removed the field causing the conflict: " + fieldCausingConflict);
          return originalEvent;
        }
        else {
          // we weren't able to find and remove the conflicting field so remove all fields for the field group we're searching within
          originalEvent.removeFieldGroup(this.fieldGroupToSearch);
          originalEvent.setNotification(NotificationKey.MappingConflict, notificationMessage + " Removed all " + this.fieldGroupToSearch + " fields.");
          return originalEvent;
        }
      }
    }
    return originalEvent;
  }

  /**
   * First tries to remove the given field from the event. If the field could not be found, we attempt to
   * remove the field group from the given field. If even this fails, we remove all but the syslog fields.
   *
   * @param originalEvent
   * @param fieldCausingConflict
   * @return
   */
  private void removeFullyQualifiedField(Event originalEvent, String fieldCausingConflict) {
    Object removedField = originalEvent.removeField(fieldCausingConflict);
    if (removedField != null) {
      int firstDot = fieldCausingConflict.indexOf(".");
      if(firstDot != -1){
          String field = fieldCausingConflict.substring(firstDot + 1, fieldCausingConflict.length());
          MappingConflictUtils.removeFieldFromFacets(originalEvent, field);
          originalEvent.setNotification(NotificationKey.MappingConflict, notificationMessage + " Removed field causing conflict: " + field);
      }
      return;
    } else {
      // we didn't find the event to remove, try to remove the field group causing the issues
      String fieldGroupName = fieldCausingConflict.split("\\.")[0];
      if(originalEvent.containsFieldGroup(fieldGroupName)) {
        originalEvent.removeFieldGroup(fieldGroupName);
        originalEvent.setNotification(NotificationKey.MappingConflict, notificationMessage + " Removed all " + fieldGroupName + " fields.");
        return;
      }
    }

    // If we didn't find a field or field group to remove, just remove everything except for syslog
    MappingConflictUtils.removeAllButSyslogFields(originalEvent);
    originalEvent.setNotification(NotificationKey.MappingConflict, notificationMessage + " Removed all fields.");
  }

  /**
   * Since we don't have a fully qualified field name, we must search within the event hoping to find the field nested within
   * the field group that was passed in via the constructor. If we don't find the field, remove the whole field group.
   *
   * @param fieldGroup
   * @param fieldCausingConflict
   * @return
   */
  @SuppressWarnings("unchecked")
  private Object searchAndRemoveField(Event originalEvent, Map<String, Object> fieldGroup, String fieldCausingConflict, List<String> path) {
    if (fieldGroup == null) {
        return null;
    }

    for (Map.Entry<String, Object> field : fieldGroup.entrySet()) {
        if (field.getKey().equals(fieldCausingConflict)) {
            path.add(field.getKey());
            MappingConflictUtils.removeFieldFromFacets(originalEvent, path);
            return fieldGroup.remove(field.getKey());
        }
    }

    // if we've made it this far, we haven't removed any fields yet. We must recurse now.
    for(Map.Entry<String, Object> field : fieldGroup.entrySet()) {
      Object recursedRemovedField = null;
      if(field.getValue() instanceof Map) {
        path.add(field.getKey());
        recursedRemovedField = searchAndRemoveField(originalEvent, (Map<String, Object>) field.getValue(), fieldCausingConflict, path);
        path.remove(path.size()-1);
      }
      if(recursedRemovedField != null) {
        // we managed to remove a field so break out of the recursion
        return recursedRemovedField;
      }
    }
    return null;
  }

  /**
   * Get the field which caused the mapping conflict.
   *
   * @param exceptionMessage
   * @return null if no field could be determined
   */
  private String getConflictingFieldFromMessage(String exceptionMessage) {
    String field = null;
    for(Pattern p : patterns) {
      Matcher matcher = p.matcher(exceptionMessage);
      if(matcher.find()) {
        field = matcher.group(1);
        //field = MappingConflictUtils.stripCidFromField(cid, field);
        break;
      }
    }
    return field;
  }
}
