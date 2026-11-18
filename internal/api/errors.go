package api

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"

	"github.com/Yigidosantos9/insider-notification-system/internal/domain"
	"github.com/Yigidosantos9/insider-notification-system/internal/store"
)

const maxBodyBytes = 10 << 20 // 10 MB: comfortably fits a 1000-item batch

type errorBody struct {
	Error apiError `json:"error"`
}

type apiError struct {
	Code    string              `json:"code"`
	Message string              `json:"message"`
	Details []domain.FieldError `json:"details,omitempty"`
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, status int, code, msg string, details ...domain.FieldError) {
	writeJSON(w, status, errorBody{Error: apiError{Code: code, Message: msg, Details: details}})
}

func badRequest(w http.ResponseWriter, msg string) {
	writeError(w, http.StatusBadRequest, "bad_request", msg)
}

// writeStoreError maps domain/store errors to HTTP responses.
func (s *server) writeStoreError(w http.ResponseWriter, r *http.Request, err error) {
	var ve *domain.ValidationError
	switch {
	case errors.As(err, &ve):
		writeError(w, http.StatusUnprocessableEntity, "validation_failed", "request validation failed", ve.Fields...)
	case errors.Is(err, store.ErrNotFound):
		writeError(w, http.StatusNotFound, "not_found", "resource not found")
	case errors.Is(err, store.ErrInvalidState):
		writeError(w, http.StatusConflict, "invalid_state", "operation not allowed in the notification's current status")
	case errors.Is(err, store.ErrIdempotencyConflict):
		writeError(w, http.StatusConflict, "idempotency_conflict", "idempotency key was already used with a different payload")
	case errors.Is(err, store.ErrTemplateNameTaken):
		writeError(w, http.StatusConflict, "template_name_taken", "a template with this name already exists")
	case errors.Is(err, store.ErrInvalidCursor):
		badRequest(w, "invalid cursor")
	default:
		s.logger(r).Error("request failed", "error", err)
		writeError(w, http.StatusInternalServerError, "internal", "internal server error")
	}
}

// decodeJSON strictly decodes the body into dst: unknown fields, trailing data
// and oversized bodies are rejected. It writes the error response itself and
// returns false on failure.
func decodeJSON(w http.ResponseWriter, r *http.Request, dst any) bool {
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, maxBodyBytes))
	dec.DisallowUnknownFields()
	err := dec.Decode(dst)
	if err == nil && dec.Decode(&struct{}{}) != io.EOF {
		err = errors.New("body must contain a single JSON object")
	}
	if err == nil {
		return true
	}
	var tooBig *http.MaxBytesError
	if errors.As(err, &tooBig) {
		writeError(w, http.StatusRequestEntityTooLarge, "body_too_large", "request body exceeds 10MB")
		return false
	}
	badRequest(w, "invalid JSON body: "+err.Error())
	return false
}
