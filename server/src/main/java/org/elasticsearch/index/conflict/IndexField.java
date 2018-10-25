package org.elasticsearch.index.conflict;

public enum IndexField {
  // Loggly specific fields
  //
  ID("_id")
  , REC_TIMESTAMP("_rects")
  , REC_SEQUENCE("_recseq")
  , REF_TIMESTAMP("_refts")
  , IDX_TIMESTAMP("_idxts")
  , CUSTOMER_ID("_custid")
  , SENDER_IP("_senderip")
  , TAG("tag")
  , LOG_HEADER("_loghdr")
  , LOG_MESSAGE("_logmsg")
  , UNPARSED("_unparsed")
  , UNPARSED_MESSAGE("_unparsedmsg")
  , LOG_TYPE("logtype")
  , LOG_SIZE("_logsize")
  , PARSE_SIZE("_parsesize")
  , PARSER("_parser")
  , SAMPLE("_sample")
  , SOURCE("_source")
  , NUMERIC_FIELDS("numeric")
  , FACET_FIELDS("facet")
  , OTHER_FIELDS("other")
  , F_NAMES("_fnames")
  , LOGGLY_NOTIFICATIONS("LogglyNotifications")
  , LEGACY_NOTIFICATIONS("notifications")
  , NOTIFICATION_TYPE("type")
  , NOTIFICATION_MESSAGE("message")
  , COST_BYTES("_costbytes")

  // ElasticSearch fields, for convenience
  , TIMESTAMP("_timestamp")
  , TYPE("_type")
  , EVENT("event") // placeholder inserted by TS while returning to FE
  , FIELDS_COUNT("_fstats")
  , FIELDS_INDEXED("indexed")
  , TOTAL_FIELDS("total")
  ;

  public final String name;

  IndexField(String name) {
    this.name = name;
  }
}
