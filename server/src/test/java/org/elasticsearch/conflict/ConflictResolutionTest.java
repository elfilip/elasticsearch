package org.elasticsearch.conflict;

import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.conflict.*;
import org.elasticsearch.test.ESTestCase;
import org.junit.Before;

import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;

public class ConflictResolutionTest extends ESTestCase {

    private static String CUSTID = "12345";

    private MappingExceptionProcessor mappingExceptionProcessor = new MappingExceptionProcessor();
    private Event EVENT;

    public void testRemoveSingleField() {
        FailedEvent fe = new FailedEvent(EVENT, "MapperParsingException[failed to parse [12345.json.field1]]; nested: IllegalStateException[Can't get text on a START_OBJECT at 1:5699]");
        mappingExceptionProcessor.process(fe);
        assertThat(checkIfPathExists(fe.getOriginalEvent().getFieldGroups(), "12345.json.field1"), equalTo(false));
        assertThat(checkIfNotificationExists(fe.getOriginalEvent().getFieldGroups(),
            NotificationKey.MappingConflict.name(),
            "Field originally sent as one type and later sent as new type. Removed field causing conflict: json.field1"), equalTo(true));
        assertThat(checkIfFacetExists(fe.getOriginalEvent().getFieldGroups(), "facet", "json.field1"), equalTo(false));
        assertThat(checkIfFacetExists(fe.getOriginalEvent().getFieldGroups(), "numeric", "json.field1"), equalTo(false));
    }

    public void testObjectToField() {
        FailedEvent fe = new FailedEvent(EVENT, "MapperParsingException[object mapping for [index.name] tried to parse field [inner1] as object, but found a concrete value]");
        mappingExceptionProcessor.process(fe);
        assertThat(checkIfPathExists(fe.getOriginalEvent().getFieldGroups(), "12345.json.inner1"), equalTo(false));
        assertThat(checkIfPathExists(fe.getOriginalEvent().getFieldGroups(), "12345.json.field1"), equalTo(true));
        assertThat(checkIfFacetExists(fe.getOriginalEvent().getFieldGroups(), "facet", "json.inner1.latitude"), equalTo(false));
        assertThat(checkIfNotificationExists(fe.getOriginalEvent().getFieldGroups(),
            NotificationKey.MappingConflict.name(),
            "Field originally sent as an object and later sent as a concrete value. Removed the field causing the conflict: inner1"), equalTo(true));

    }

    public void testRemoveSingleUnqualified() {
        FailedEvent fe = new FailedEvent(EVENT, "MapperParsingException[object mapping [field1] trying to serialize a value with no field associated with it, current value [derp]]");
        mappingExceptionProcessor.process(fe);
        assertThat(checkIfPathExists(fe.getOriginalEvent().getFieldGroups(), "12345.json.field1"), equalTo(false));
        assertThat(checkIfPathExists(fe.getOriginalEvent().getFieldGroups(), "12345.json.field2"), equalTo(true));
        assertThat(checkIfFacetExists(fe.getOriginalEvent().getFieldGroups(), "facet", "json.field1"), equalTo(false));
        assertThat(checkIfFacetExists(fe.getOriginalEvent().getFieldGroups(), "facet", "json.field2"), equalTo(true));
    }

    public void testRemoveSingleUnqualifiedObject() {
        FailedEvent fe = new FailedEvent(EVENT, "MapperParsingException[object mapping [inner1] trying to serialize a value with no field associated with it, current value [derp]]");
        mappingExceptionProcessor.process(fe);
        assertThat(checkIfPathExists(fe.getOriginalEvent().getFieldGroups(), "12345.json.inner1"), equalTo(false));
        assertThat(checkIfPathExists(fe.getOriginalEvent().getFieldGroups(), "12345.json.field2"), equalTo(true));
        assertThat(checkIfFacetExists(fe.getOriginalEvent().getFieldGroups(), "facet", "json.inner1.latitude"), equalTo(false));
        assertThat(checkIfFacetExists(fe.getOriginalEvent().getFieldGroups(), "facet", "json.inner1.location"), equalTo(false));
        assertThat(checkIfFacetExists(fe.getOriginalEvent().getFieldGroups(), "facet", "json.field2"), equalTo(true));
    }

    public void testRemoveSingleUnqualifiedEOF() {
        FailedEvent fe = new FailedEvent(EVENT, "MapperParsingException[object mapping for [index.name] tried to parse field [field1] as object, but got EOF, has a concrete value been provided to it?]");
        mappingExceptionProcessor.process(fe);
        assertThat(checkIfPathExists(fe.getOriginalEvent().getFieldGroups(), "12345.json.field1"), equalTo(false));
        assertThat(checkIfPathExists(fe.getOriginalEvent().getFieldGroups(), "12345.json.field2"), equalTo(true));
        assertThat(checkIfFacetExists(fe.getOriginalEvent().getFieldGroups(), "facet", "json.field1"), equalTo(false));
        assertThat(checkIfFacetExists(fe.getOriginalEvent().getFieldGroups(), "facet", "json.field2"), equalTo(true));
    }

    public void testRemoveJSON() {
        FailedEvent fe = new FailedEvent(EVENT, "MapperParsingException[object mapping for [index.name] tried to parse as object, but got EOF, has a concrete value been provided to it?]");
        mappingExceptionProcessor.process(fe);
        assertThat(checkIfPathExists(fe.getOriginalEvent().getFieldGroups(), "12345.json.field1"), equalTo(false));
        assertThat(checkIfPathExists(fe.getOriginalEvent().getFieldGroups(), "12345.json.field2"), equalTo(false));
        assertThat(checkIfPathExists(fe.getOriginalEvent().getFieldGroups(), "12345.json.inner1"), equalTo(false));
        assertThat(checkIfPathExists(fe.getOriginalEvent().getFieldGroups(), "http.clientHost"), equalTo(true));
        assertThat(checkIfFacetExists(fe.getOriginalEvent().getFieldGroups(), "facet", "json.field1"), equalTo(false));
        assertThat(checkIfFacetExists(fe.getOriginalEvent().getFieldGroups(), "facet", "json.field2"), equalTo(false));
        assertThat(checkIfFacetExists(fe.getOriginalEvent().getFieldGroups(), "facet", "json.inner1"), equalTo(false));
        assertThat(checkIfFacetExists(fe.getOriginalEvent().getFieldGroups(), "facet", "json.field3"), equalTo(false));
        assertThat(checkIfNotificationExists(fe.getOriginalEvent().getFieldGroups(),
            NotificationKey.MappingConflict.name(),
            "Field originally sent as an object and later as a concrete value or an array. Removed all json fields."), equalTo(true));

    }

    public void testRemoveAllButSyslog() {
        FailedEvent fe = new FailedEvent(EVENT, "MapperParsingException[Unknown error]");
        mappingExceptionProcessor.process(fe);
        assertThat(checkIfPathExists(fe.getOriginalEvent().getFieldGroups(), "12345.json.field1"), equalTo(false));
        assertThat(checkIfPathExists(fe.getOriginalEvent().getFieldGroups(), "12345.json.field2"), equalTo(false));
        assertThat(checkIfPathExists(fe.getOriginalEvent().getFieldGroups(), "12345.json.inner1"), equalTo(false));
        assertThat(checkIfPathExists(fe.getOriginalEvent().getFieldGroups(), "http.clientHost"), equalTo(false));
        assertThat(checkIfPathExists(fe.getOriginalEvent().getFieldGroups(), "syslog.appName"), equalTo(true));
        assertThat(checkIfPathExists(fe.getOriginalEvent().getFieldGroups(), "_idxts"), equalTo(true));
        assertThat(checkIfFacetExists(fe.getOriginalEvent().getFieldGroups(), "facet", "json.field1"), equalTo(false));
        assertThat(checkIfFacetExists(fe.getOriginalEvent().getFieldGroups(), "facet", "json.field2"), equalTo(false));
        assertThat(checkIfFacetExists(fe.getOriginalEvent().getFieldGroups(), "facet", "json.inner1"), equalTo(false));
        assertThat(checkIfFacetExists(fe.getOriginalEvent().getFieldGroups(), "facet", "json.field3"), equalTo(false));
        assertThat(checkIfFacetExists(fe.getOriginalEvent().getFieldGroups(), "facet", "syslog.appName"), equalTo(true));
    }


    @Before
    public void createEvent() {
        Map<String, Object> source = XContentHelper.convertToMap(XContentType.JSON.xContent(),
            Thread.currentThread().getContextClassLoader().getResourceAsStream("config/data_test.json"), true);
        EVENT = new Event();
        EVENT.setFieldGroups(source);
        EVENT.setCustomerID(CUSTID);
    }

    private boolean checkIfPathExists(Map<String, Object> root, String path) {
        Map<String, Object> current = root;
        for (String name : path.split("\\.")) {
            Object next = current.get(name);
            if (next == null) {
                return false;
            }
            if(next instanceof Map) {
                current =(Map<String, Object>) next;
            }
        }
        return true;
    }

    private boolean checkIfFacetExists(Map<String, Object> root, String facet, String name) {
        Map<String, List> fnames = (Map<String, List>) root.get("_fnames");
        if (fnames == null) {
            return false;
        }
        List<String> names = fnames.get(facet);
        if (names == null) {
            return false;
        }
        return names.contains(name);
    }

    private boolean checkIfNotificationExists(Map<String, Object> root, String typeValue, String messageValue) {
        List<Map<String, Object>> notifications = (List<Map<String, Object>> )root.get(IndexField.LOGGLY_NOTIFICATIONS.name);
        for(Map<String, Object> notification : notifications){
            String type = (String) notification.get(IndexField.NOTIFICATION_TYPE.name);
            String message = (String) notification.get(IndexField.NOTIFICATION_MESSAGE.name);
            if (type != null && type.equals(typeValue) && message != null && message.equals(messageValue)) {
                return true;
            }
        }
        return false;
    }


}
