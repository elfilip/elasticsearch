package org.elasticsearch.index.conflict;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class RemoveJsonFieldsMappingResolver implements MappingConflictResolver {
  private final List<Pattern> patterns;
  private final String notificationMessage;

  /**
   * A mapping conflict resolver that just tried to remove all json fields. This
   * is useful if ES doesn't give us any information about what field caused the conflict,
   * but we knot that it happened somewhere in the json field group.
   *
   *
   * @param notificationMessage The message that is included in the notification that is attached to the event.
   *                            This message is appended with what field was removed.
   * @param regexes A list of regexes to match against the input message. If the regex matches, we remove the json fields.
   */
  public RemoveJsonFieldsMappingResolver(String notificationMessage, String... regexes) {
    this.patterns = new ArrayList<>(regexes.length);
    for(String regex : regexes) {
      patterns.add(Pattern.compile(regex));
    }
    this.notificationMessage = notificationMessage;
  }

  /**
   * Try to resolve the mapping conflict. Return null if none of the regexes matched.
   *
   * @param originalEvent
   * @param mappingExceptionMessage
   * @return
   */
  public Event resolveMappingConflict(Event originalEvent, String mappingExceptionMessage) {
    if(canResolve(mappingExceptionMessage)) {
      return resolve(originalEvent);
    } else {
      return null;
    }
  }

  private boolean canResolve(String mappingExceptionMessage) {
    for(Pattern p : patterns) {
      Matcher matcher = p.matcher(mappingExceptionMessage);
      if(matcher.find()) {
        return true;
      }
    }
    return false;
  }

  private Event resolve(Event originalEvent) {
    if(!originalEvent.containsFieldGroup(originalEvent.getCustomerID())){
        MappingConflictUtils.removeAllButSyslogFields(originalEvent);
    }else{
        originalEvent.removeFieldGroup(originalEvent.getCustomerID());
    }
    MappingConflictUtils.removeFieldFromFacets(originalEvent, FieldGroups.JSON.getName());
    return originalEvent;
  }


}

