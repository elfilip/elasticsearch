package org.elasticsearch.index.conflict;

public enum NotificationKey {
  MaxFieldsLimitReached,
  MaxTagsLimitReached,
  InvalidTags,
  TimestampBelowFloor,
  TimestampAboveCeiling,
  ScrubbedKeys,
  ScrubbedValues,
  TimezoneMissing,
  EventClockOrTimezoneWrong,
  SyslogClockOrTimezoneWrong,
  SyslogClockDrift, //Clock drift wrt to reception timestamp
  EventClockDrift, //Clock drift wrt to reception timestamp
  FieldNameMaxSizeReached,
  MappingConflict,
  MaxCustomFieldsLimitReached,
  DerivedFieldTypeMismatch,
  InvalidTimestamp,
  FieldDepthLimitReached
  ;
}
