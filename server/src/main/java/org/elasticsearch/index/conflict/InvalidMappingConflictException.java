package org.elasticsearch.index.conflict;

public class InvalidMappingConflictException extends RuntimeException {

  private static final long serialVersionUID = 1L;

  public InvalidMappingConflictException(Exception cause) {
    super(cause);
  }

  public InvalidMappingConflictException(String msg, Exception cause) {
    super(msg, cause);
  }

  public InvalidMappingConflictException(String msg) {
    super(msg);
  }
}
