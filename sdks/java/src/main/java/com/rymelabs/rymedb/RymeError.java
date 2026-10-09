package com.rymelabs.rymedb;

/** Raised when the rymeDB HTTP API returns a non-success response. */
public final class RymeError extends RuntimeException {
    private final int status;
    private final String body;

    public RymeError(int status, String body) {
        super("rymeDB HTTP " + status + ": " + body);
        this.status = status;
        this.body = body;
    }

    public int status() {
        return status;
    }

    public String body() {
        return body;
    }
}
