package org.elasticsearch.index.conflict;

/**
 * Log Field groups used by parser, indexer and other components
 */
public enum FieldGroups {

  SYSLOG("syslog"), // syslog header extracted KV
  PARSER("parser"), // Parser meta KV
  JSON("json"), // json values
  KEY_VALUES("kv"), // kv fields extracted from log text
  JAVA_GC("javagc"),  // gc logs from java process
  JAVA("java"),
  SYSLOG_SD("sd"),
  HTTP("http"),
  APACHE("apache"),
  NGINX("nginx"),
  NODEJS("nodejs"),
  SYSTEM("system"),
  HAPROXY("haproxy"),
  HEROKU("heroku"),
  RAILS("rails"),
  MONGO("mongo"),
  MYSQL("mysql"),
  PHP("php"),
  WINDOWS("windows"),
  CUSTOM("derived")
  ;

  public final String name;

  FieldGroups(String name) {
    this.name = name;
  }

  public String getName() {
    return name;
  }
}
