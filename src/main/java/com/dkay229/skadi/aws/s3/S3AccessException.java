package com.dkay229.skadi.aws.s3;

public class S3AccessException extends RuntimeException {
    public S3AccessException(String message, Throwable cause) { super(message, cause); }
    public S3AccessException(String message) { super(message); }
}

