package com.company.dbreactjmix.metadata.controller;

import com.mongodb.MongoCommandException;
import com.mongodb.MongoException;
import com.mongodb.MongoSecurityException;
import com.mongodb.MongoSocketOpenException;
import com.mongodb.MongoTimeoutException;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.server.ResponseStatusException;

import java.time.OffsetDateTime;
import java.util.LinkedHashMap;
import java.util.Map;

@RestControllerAdvice
public class GlobalExceptionHandler {

    @ExceptionHandler(IllegalArgumentException.class)
    public ResponseEntity<Map<String, Object>> handleBadRequest(IllegalArgumentException e) {
        return build(HttpStatus.BAD_REQUEST, e.getMessage());
    }

    @ExceptionHandler(ResponseStatusException.class)
    public ResponseEntity<Map<String, Object>> handleResponseStatus(ResponseStatusException e) {
        return build(HttpStatus.valueOf(e.getStatusCode().value()), e.getReason());
    }

    @ExceptionHandler(MongoCommandException.class)
    public ResponseEntity<Map<String, Object>> handleMongoCommandException(MongoCommandException e) {
        return build(HttpStatus.BAD_REQUEST, classifyMongoCommandException(e));
    }

    @ExceptionHandler(MongoSecurityException.class)
    public ResponseEntity<Map<String, Object>> handleMongoSecurityException(MongoSecurityException e) {
        return build(HttpStatus.UNAUTHORIZED, "MongoDB authentication failed. Kiểm tra username/password hoặc auth database. Chi tiết: " + e.getMessage());
    }

    @ExceptionHandler(MongoSocketOpenException.class)
    public ResponseEntity<Map<String, Object>> handleMongoSocketException(MongoSocketOpenException e) {
        return build(HttpStatus.BAD_REQUEST, "Không mở được kết nối MongoDB. Kiểm tra host, port, Network Access của Atlas hoặc firewall. Chi tiết: " + e.getMessage());
    }

    @ExceptionHandler(MongoTimeoutException.class)
    public ResponseEntity<Map<String, Object>> handleMongoTimeoutException(MongoTimeoutException e) {
        return build(HttpStatus.BAD_REQUEST, "MongoDB connection timed out. Kiểm tra Network Access của Atlas, DNS, hoặc cluster đang sẵn sàng. Chi tiết: " + e.getMessage());
    }

    @ExceptionHandler(MongoException.class)
    public ResponseEntity<Map<String, Object>> handleMongoException(MongoException e) {
        return build(HttpStatus.BAD_REQUEST, "Không kết nối được MongoDB. Kiểm tra host/port, Mongo service đang chạy, Docker port mapping, firewall và auth database. Chi tiết: " + e.getMessage());
    }

    @ExceptionHandler(Exception.class)
    public ResponseEntity<Map<String, Object>> handleUnexpected(Exception e) {
        return build(HttpStatus.INTERNAL_SERVER_ERROR, e.getMessage());
    }

    private ResponseEntity<Map<String, Object>> build(HttpStatus status, String message) {
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("timestamp", OffsetDateTime.now().toString());
        body.put("status", status.value());
        body.put("error", status.getReasonPhrase());
        body.put("message", message);
        return ResponseEntity.status(status).body(body);
    }

    private String classifyMongoCommandException(MongoCommandException e) {
        if (e.getCode() == 292) {
            return "MongoDB query quá nặng cho bộ nhớ tạm. Hãy thêm $limit sớm hơn, tránh sort trên tập lớn, hoặc dùng index phù hợp. Chi tiết: " + e.getErrorMessage();
        }
        if (e.getCode() == 13 || e.getCode() == 8000) {
            return "MongoDB permission denied. User hiện tại không có quyền chạy lệnh này trên database/collection đã chọn. Chi tiết: " + e.getErrorMessage();
        }
        return "MongoDB command failed: " + e.getErrorMessage();
    }
}
