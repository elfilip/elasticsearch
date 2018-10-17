package org.elasticsearch.index.conflict;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.UUID;

/**
 * An Event represents a log entry. Unless specified explicitly, fields in this class are extracted
 * by the deserializer. If they are set by Parser or any other stage they need to be mentioned
 */
public class Event implements Serializable {

    // metadata for each event is a 16 bit value. See the wiki for details
    // bit 0 is LSB (i.e. rightmost), bit 15 is MSB (leftmost)
    public static enum MetaData {
        isTCP(0, "TCP"),
        isUDP(1, "UDP"),
        isHTTP(2, "HTTP"),
        hasAuth(3, "AUTH_SET"),
        isSecure(4, "SECURE"),
        dumpEnabled(5, "DUMP");

        private final short bit;
        public final String name;

        private MetaData(int bitNum, String name) {
            bit = (short) (1 << bitNum);
            this.name = name;
        }

        public short bit() {
            return bit;
        }

        public boolean isSet(short v) {
            return (v & bit) == bit;
        }

        public boolean isSet(Event e) {
            return isSet(e.getMetadata());
        }
    }

    private final static String NULL = "null";

    private static final long serialVersionUID = 3L;

    protected int customerID = -1;
    protected int senderIP = -1;
    protected long receptionTimestamp = -1;
    protected long referenceTimestamp = -1;
    protected short metadata = 0;
    protected UUID uuid = null;
    protected String rawPayload = null;
    protected int userLength = 0;
    protected int costBytes = 0;
    protected String auth = null;
    protected int recvPort = -1;
    protected String sender = null;
    protected HashSet<String> tags = new HashSet<>(2);  // set by Parser or in case of HTTP, by deserializer
    protected byte version = -1;
    protected String logHeader;  // set by Parser
    protected String logMessage; // set by Parser
    protected String unparsedMessage; // set by Parser, remaining text of log which was not parsed
    protected boolean unparsed = true; // init to true, set by parser
    protected List<Object> traces = new ArrayList<>(2);

    protected Map<String,Object> fieldGroups = new HashMap<>(2); // Set by Parser
    protected Set<String> logTypes = new HashSet<>(2); // Field group types set by Parser

    public Event() {

    }

    public String getAuth() {
        return auth;
    }

    public int getCustomerID() {
        return customerID;
    }

    public short getMetadata() {
        return metadata;
    }

    public String getRawPayload() {
        return rawPayload;
    }

    public int getUserLength() {
        return userLength;
    }

    public int getCostBytes() {
        return costBytes;
    }

    public long getReceptionTimestamp() {
        return receptionTimestamp;
    }

    /**
     * Returns a number which is always greater for Event B than for Event A IF
     * Event B was unambiguously received after Event A. If Event B was not
     * unambiguously received after Event A, no guarantees are made regarding the
     * value of the number returned.
     */
    public long getReceptionSequence() {
        return this.uuid.timestamp();
    }

    public int getRecvPort() {
        return recvPort;
    }

    public long getReferenceTimestamp() {
        return referenceTimestamp;
    }

    public String getSender() {
        return sender;
    }

    public HashSet<String> getTags() {
        return tags;
    }

    public int getSenderIP() {
        return senderIP;
    }


    public UUID getUUID() {
        return uuid;
    }

    public byte getVersion() {
        return version;
    }

    /**
     * Syslog header exactly as received, null if non syslog event or header not
     * compliant
     * @return
     */
    public String getLogHeader() {
        return logHeader;
    }

    /**
     * If syslog event, syslog message part. Otherwise entire event, exactly as received
     * @return
     */
    public String getLogMessage() {
        return logMessage;
    }

    /**
     * Returns the remainder of the log message which was not parsed by the parser
     * This may be null if the entire event was parsed. This is not indexed
     * @return
     */
    public String getUnparsedMessage() {
        return unparsedMessage;
    }

    public Set<String> getLogTypes() {
        return logTypes;
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

    public List<Object> getTraces() {
        return traces;
    }

    public boolean isUnparsed() {
        return unparsed;
    }

    public void setAuth(String auth) {
        this.auth = auth;
    }

    public void setCustomerID(int value) {
        this.customerID = value;
    }

    public void setMetadata(short value) {
        this.metadata = value;
    }

    public void setRawPayload(String value) {
        this.rawPayload = value;
    }

    public void setUserLength(int value) {
        this.userLength = value;
    }

    public void setCostBytes(int value) {
        this.costBytes = value;
    }

    public void setReceptionTimestamp(long value) {
        this.receptionTimestamp = value;
    }

    public void setRecvPort(int recvPort) {
        this.recvPort = recvPort;
    }

    public void setReferenceTimestamp(long value) {
        this.referenceTimestamp = value;
    }

    public void setSender(String sender) {
        this.sender = sender;
    }

    public void setTags(HashSet<String> tags) {
        this.tags = tags;
    }

    public void setSenderIP(int value) {
        this.senderIP = value;
    }




    public void setVersion(byte version) {
        this.version = version;
    }



    public void setLogHeader(String logHeader) {
        this.logHeader = logHeader;
    }

    public void setLogMessage(String logMessage) {
        this.logMessage = logMessage;
    }

    public void setUnparsedMessage(String unparsedMessage) {
        this.unparsedMessage = unparsedMessage;
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

    public void setLogTypes(Set<String> logTypes) {
        this.logTypes = logTypes;
    }

    public void addLogType(final String logType) {
        this.logTypes.add(logType);
    }

    public void setUnparsed(boolean v) {
        this.unparsed = v;
    }



    @Override
    public String toString() {
        return "Event@" + Integer.toHexString(hashCode()) + "["
            + "auth=" + auth
            + ", uuid=" + uuid
            + ", customerID=" + customerID
            + ", senderIP=" + senderIP
            + ", receptionTimestamp=" + receptionTimestamp
            + ", referenceTimestamp=" + referenceTimestamp
            + ", metadata=" + getMetadataString()
            + ", recvPort=" + recvPort
            + ", sender=" + sender
            + ", tags=" + tags
            + ", version=" + version
            + ", logHeader='" + logHeader + "'"
            + ", logMessage='" + logMessage + "'"
            + ", unparsedMessage='" + unparsedMessage + "'"
            + ", fieldGroups=" + fieldGroups
            + ", logTypes=" + logTypes
            + ", rawPayload=" + rawPayload
            + ", unparsed=" + unparsed
            + ", userLength=" + userLength
            + ", costBytes=" + costBytes
            + ']';
    }

    private String getMetadataString() {
        StringBuilder s = new StringBuilder("(" + metadata + ")");
        if (metadata == 0) {
            return s.append(NULL).toString();
        }
        for (MetaData m : MetaData.values()) {
            if (m.isSet(this)) {
                s.append(m.name).append(',');
            }
        }
        return s.toString();
    }

    public boolean flagSet(MetaData m) {
        return m.isSet(this);
    }

    public void setFlag(MetaData m) {
        setMetadata((short) (metadata | m.bit));
    }

    public void unSetFlag(MetaData m) {
        setMetadata((short) (metadata & ~m.bit));
    }

    public boolean isHTTP() {
        return flagSet(MetaData.isHTTP);
    }

    /**
     * Returns basic info that ID's this event like customer ID and UUID
     * Useful for debugging
     * @return
     */
    public String getBasicDebugInfo() {
        return "[uuid=" + uuid
            + ",auth=" + auth
            + ",customerID=" + customerID
            + ",rects=" + receptionTimestamp
            + ",refts=" + referenceTimestamp
            + ",senderIP=" + senderIP
            + "]";
    }

    public void clearFieldGroups() {
        fieldGroups.clear();
    }
}
