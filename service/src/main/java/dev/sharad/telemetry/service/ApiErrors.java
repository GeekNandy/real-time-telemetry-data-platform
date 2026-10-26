package dev.sharad.telemetry.service;

import dev.sharad.telemetry.core.AdmissionController;
import dev.sharad.telemetry.core.Event;
import dev.sharad.telemetry.core.IngestionEngine;
import org.springframework.core.io.buffer.DataBufferLimitException;
import org.springframework.http.HttpStatus;
import org.springframework.http.ProblemDetail;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.bind.support.WebExchangeBindException;
import org.springframework.web.server.ServerWebInputException;

@RestControllerAdvice
public class ApiErrors {
    @ExceptionHandler({Event.InvalidEvent.class, WebExchangeBindException.class, ServerWebInputException.class})
    ResponseEntity<ProblemDetail> invalidEvent(Exception error) {
        if (error instanceof ServerWebInputException input
                && input.getMostSpecificCause() instanceof DataBufferLimitException) return tooLarge();
        return problem(HttpStatus.BAD_REQUEST, "Event does not match the supported contract.", false);
    }

    @ExceptionHandler(DataBufferLimitException.class)
    ResponseEntity<ProblemDetail> tooLarge() {
        return problem(HttpStatus.PAYLOAD_TOO_LARGE, "Request exceeds the payload limit.", false);
    }

    @ExceptionHandler(AdmissionController.Rejected.class)
    ResponseEntity<ProblemDetail> rejected(AdmissionController.Rejected error) {
        if (error.reason() == AdmissionController.Reason.DRAINING) {
            return problem(HttpStatus.SERVICE_UNAVAILABLE, "Service is draining. Retry with the same eventId.", true);
        }
        return problem(HttpStatus.TOO_MANY_REQUESTS, "Admission capacity exhausted. Retry with the same eventId.", true);
    }

    @ExceptionHandler(IngestionEngine.BrokerFailure.class)
    ResponseEntity<ProblemDetail> brokerFailure() {
        return problem(HttpStatus.SERVICE_UNAVAILABLE, "Acceptance is unconfirmed. Retry with the same eventId.", true);
    }

    private static ResponseEntity<ProblemDetail> problem(HttpStatus status, String detail, boolean retry) {
        var builder = ResponseEntity.status(status);
        if (retry) builder.header("Retry-After", "1");
        return builder.body(ProblemDetail.forStatusAndDetail(status, detail));
    }
}
