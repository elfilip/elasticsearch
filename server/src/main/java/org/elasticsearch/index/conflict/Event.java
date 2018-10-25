package org.elasticsearch.index.conflict;

import java.io.Serializable;
import java.util.*;

import static org.elasticsearch.index.conflict.IndexField.NOTIFICATION_TYPE;

/**
 * An Event represents a log entry. Unless specified explicitly, fields in this class are extracted
 * by the deserializer. If they are set by Parser or any other stage they need to be mentioned
 */
public class Event implements Serializable {

    private final static String NULL = "null";

    private static final long serialVersionUID = 3L;

    protected String customerID = "-1";
    protected Map<String,Object> fieldGroups = new HashMap<>(2); // Set by Parser
    protected String[] notifications = new String[NotificationKey.values().length];


    public Event() {

    }

    public boolean containsFieldGroup(String groupName) {
        return fieldGroups != null && fieldGroups.containsKey(groupName);
    }

    /**
     * Returns a field group map given the group name
     *
     * @param groupName
     * @return
     */
    public Map<String,Object> getFieldGroup(String groupName) {
        if (!fieldGroups.containsKey(groupName) || fieldGroups.get(groupName) == null) {
            fieldGroups.put(groupName, new HashMap<String,Object>());
        }
        return (Map<String, Object>) fieldGroups.get(groupName);
    }

    /**
     * Removes a field group map given the group name
     *
     * @param groupName
     * @return the removed field group or null if the field group did not exist
     */
    public Map<String,Object> removeFieldGroup(String groupName) {
        if (fieldGroups.containsKey(groupName) && fieldGroups.get(groupName) != null) {
            return (Map<String, Object>) fieldGroups.remove(groupName);
        }
        return null;
    }

    /**
     * Returns the field when given a period separated field path, i.e. json.field1.value,
     * or null if the field doesn't exist. The logic assumes the path will have at least two
     * elements.
     *
     * @param fieldName
     * @return
     */
    @SuppressWarnings("unchecked")
    public Object getField(String fieldName) {
        final String[] fieldPath = StringUtil.DOT_MATCHER.split(fieldName);
        if(fieldPath.length < 2) {
            return null;
        }

        if(!containsFieldGroup(fieldPath[0])) {
            return null;
        }

        Map<String, Object> fieldGroups = getFieldGroup(fieldPath[0]);

        Object payloadValue = null;
        for(int i = 1; i < fieldPath.length; i++) {
            if(fieldGroups == null) {
                // the path didn't lead to a field that exists
                break;
            }
            if(i == fieldPath.length - 1) {
                payloadValue = fieldGroups.get(fieldPath[i]);
            } else {
                if(fieldGroups.get(fieldPath[i]) instanceof Map) {
                    fieldGroups = (Map<String, Object>) fieldGroups.get(fieldPath[i]);
                } else {
                    // there are still nodes left in the path, but we've ran out of map to traverse
                    // therefore, the field doesn't exist
                    fieldGroups = null;
                }
            }
        }
        return payloadValue;
    }

    /**
     * Removes the given field specified by a period separated field path, i.e. json.field1.value,
     * or null if the field doesn't exist. The logic assumes the path will have at least two
     * elements.
     *
     * @param fieldName
     * @return the value that was remove or null if the field could not be found
     */
    @SuppressWarnings("unchecked")
    public Object removeField(String fieldName) {
        final String[] fieldPath = StringUtil.DOT_MATCHER.split(fieldName);
        if(fieldPath.length < 2) {
            return null;
        }

        if(!containsFieldGroup(fieldPath[0])) {
            return null;
        }

        Map<String, Object> fieldGroups = getFieldGroup(fieldPath[0]);

        for(int i = 1; i < fieldPath.length; i++) {
            if(fieldGroups == null) {
                // the path didn't lead to a field that exists
                return null;
            }
            if(i == fieldPath.length - 1) {
                return fieldGroups.remove(fieldPath[i]);
            } else {
                if(fieldGroups.get(fieldPath[i]) instanceof Map) {
                    fieldGroups = (Map<String, Object>) fieldGroups.get(fieldPath[i]);
                }
            }
        }

        return null;
    }

    /**
     * Returns all field groups
     * @return
     */
    public Map<String,Object> getFieldGroups() {
        return fieldGroups;
    }

    public void setFieldGroups(Map<String,Object> fieldGroups) {
        this.fieldGroups = fieldGroups;
    }

    /**
     * Gets all the field groups for this event, including custom fields
     *
     * @return
     */
    public Set<String> getFieldGroupNames() {
        return fieldGroups.keySet();
    }



    public void setCustomerID(String value) {
        this.customerID = value;
    }

    /**
     * Sets a field group given a group name
     *
     * @param groupName
     * @param map
     */
    public void setFieldGroup(String groupName, Map<String,Object> map) {
        fieldGroups.put(groupName, map);
    }

    @Override
    public String toString() {
        return "Event@" + Integer.toHexString(hashCode()) + "["
            + ", customerID=" + customerID
            + ", fieldGroups=" + fieldGroups;
    }

    public void clearFieldGroups() {
        fieldGroups.clear();
    }

    public String getCustomerID() {
        return customerID;
    }

    public String getNotification(NotificationKey key) {
        // to catch any serialization/deserialization issues with increased array size
        if (key.ordinal() >= notifications.length){
            return null;
        }
        return notifications[key.ordinal()];
    }

    public void setNotification(NotificationKey key, String message) {
        if (key.ordinal() >= notifications.length) {
            notifications = Arrays.copyOf(notifications, key.ordinal() + 1);
        }
        notifications[key.ordinal()] = message;
    }

    public void applyNotifications() {
        List<Map<String, ?>> notifications = (List<Map<String, ?>>) getFieldGroups().get(IndexField.LOGGLY_NOTIFICATIONS.name);
        if(notifications == null){
            notifications = new LinkedList<>();
            fieldGroups.put(IndexField.LOGGLY_NOTIFICATIONS.name, notifications);
        }
        for (NotificationKey n: NotificationKey.values()) {
            String msg = getNotification(n);
            if (msg != null && !msg.isEmpty()) {
                Map<String, String> notificationObject = new HashMap<>();
                notificationObject.put(NOTIFICATION_TYPE.name, n.name());
                notificationObject.put(IndexField.NOTIFICATION_MESSAGE.name, msg);
                notifications.add(notificationObject);
                // Only facet notification type
                MappingConflictUtils.addFieldToFacets(this, IndexField.NOTIFICATION_TYPE.name);
            }
        }




    }
}
