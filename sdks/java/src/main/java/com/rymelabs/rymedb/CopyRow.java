package com.rymelabs.rymedb;

/** A key/value row accepted by the SQL copy endpoint. */
public record CopyRow(String key, String value) {}
