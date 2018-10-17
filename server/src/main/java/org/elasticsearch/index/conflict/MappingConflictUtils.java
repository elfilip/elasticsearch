package org.elasticsearch.index.conflict;

import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentType;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class MappingConflictUtils {

  // constants for loading the mapping conflicts file
  private final static String MAPPING_CONFLICT_FILE = "mapping-conflicts.json";
  private final static String MAPPING_CONFLICTS_FIELD = "mapping_conflicts";
  private final static String CONFLICT_TYPE_FIELD = "type";
  private final static String REGEXES_FIELD = "regexes";
  private final static String NOTIFICATION_FIELD = "notification";
  private final static String FIELD_IS_FULLY_QUALIFIED = "field_is_fully_qualified";
  private final static String FIELD_GROUP_CONTAINING_FIELD = "field_group_containing_field";

  // Different types of mapping conflict resolvers go here, i.e. mapping conflicts containing no fields or mapping conflicts
  // containing a type and a field, resolves that rename fields, etc.
  private enum ResolverType {
    REMOVE_SINGLE_FIELD, REMOVE_JSON_FIELDS
  }

  /**
   * Load the mapping conflict definitions from the mapping conflicts file. Mapping conflicts are defined in this
   * file to make is super easy to add new types of mapping conflict rules.
   *
   * @return list of MappingConflictResolvers
   */
  @SuppressWarnings("unchecked")
  public static ArrayList<MappingConflictResolver> initMappingConflictResolvers() {
    try {
     // List<Map<String,Object>> mappingConflicts = (List<Map<String, Object>>) JsonUtil.createMapFromFile(MAPPING_CONFLICT_FILE).get(MAPPING_CONFLICTS_FIELD);
        List<Map<String, Object>> mappingConflicts = (List<Map<String, Object>>) XContentHelper.convertToMap(XContentType.JSON.xContent(),
            Thread.currentThread().getContextClassLoader().getResourceAsStream("config/mapping-conflicts.json"), true).get(MAPPING_CONFLICTS_FIELD);
      ArrayList<MappingConflictResolver> resolvers = new ArrayList<>(mappingConflicts.size());
      for(Map<String,Object> mappingConflict : mappingConflicts) {
        MappingConflictResolver resolver;
        ResolverType type = ResolverType.valueOf((String) mappingConflict.get(CONFLICT_TYPE_FIELD));
        List<String> regexes = (List<String>) mappingConflict.get(REGEXES_FIELD);
        if(regexes == null || regexes.isEmpty()) {
          throw new InvalidMappingConflictException(ResolverType.REMOVE_SINGLE_FIELD.name()+" type conflict must specify regex to use to parse a mapping conflict.");
        }
        String notificationMessage = (String) mappingConflict.get(NOTIFICATION_FIELD);
        if(notificationMessage == null || notificationMessage.isEmpty()) {
          throw new InvalidMappingConflictException(ResolverType.REMOVE_SINGLE_FIELD.name()+" type conflict must specify a notification message.");
        }

        String[] regexesArray = new String[regexes.size()];

        switch(type) {
          case REMOVE_SINGLE_FIELD:
            Boolean fieldIsFullyQualified = (Boolean) mappingConflict.get(FIELD_IS_FULLY_QUALIFIED);
            if(fieldIsFullyQualified == null) {
              throw new InvalidMappingConflictException(FIELD_IS_FULLY_QUALIFIED+" must be present.");
            }

            if(!fieldIsFullyQualified) {
              String fieldGroupContainingField = (String) mappingConflict.get(FIELD_GROUP_CONTAINING_FIELD);
              if(fieldIsFullyQualified == null) {
                throw new InvalidMappingConflictException("If "+FIELD_IS_FULLY_QUALIFIED+" is false, "+FIELD_GROUP_CONTAINING_FIELD+" must be present.");
              }
              resolver = new RemoveSingleFieldMappingResolver(notificationMessage, fieldGroupContainingField, regexes.toArray(regexesArray));
              resolvers.add(resolver);
            } else {
              resolver = new RemoveSingleFieldMappingResolver(notificationMessage, regexes.toArray(regexesArray));
              resolvers.add(resolver);
            }
            break;
          case REMOVE_JSON_FIELDS:
            resolver = new RemoveJsonFieldsMappingResolver(notificationMessage, regexes.toArray(regexesArray));
            resolvers.add(resolver);
            break;
          default:
            throw new InvalidMappingConflictException("Unrecognized mapping conflict type.");
        }
      }
      return resolvers;
    } catch(Exception e) {
      throw new RuntimeException("Could not initialize mapping conflicts: ", e);
    }
  }

  /**
   * Assuming a field is in the format: 1234.json.field1, strip the cid from the field name
   *
   * @param field
   * @return
   */
  public static String stripCidFromField(int cid, String field) {
    if(field.startsWith(cid+".")) {
      return field.substring(field.indexOf(".")+1);
    } else {
      // could not strip cid because there was no cid
      return field;
    }
  }

  public static void removeAllButSyslogFields(Event originalEvent) {
    // just save the syslog parsed out fields and clear everythign else
    Map<String, Object> syslog = originalEvent.getFieldGroup(FieldGroups.SYSLOG.name);
    originalEvent.clearFieldGroups();
    originalEvent.setFieldGroup(FieldGroups.SYSLOG.name, syslog);
  }
}
