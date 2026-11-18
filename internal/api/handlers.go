package api

import (
	"fmt"
	"net/http"
	"strconv"
	"time"

	"github.com/google/uuid"

	"github.com/Yigidosantos9/insider-notification-system/internal/domain"
	"github.com/Yigidosantos9/insider-notification-system/internal/logging"
	"github.com/Yigidosantos9/insider-notification-system/internal/store"
)

const (
	defaultPageSize = 20
	maxPageSize     = 100
)

func (s *server) createNotification(w http.ResponseWriter, r *http.Request) {
	var n domain.NewNotification
	if !decodeJSON(w, r, &n) {
		return
	}
	if key := r.Header.Get("Idempotency-Key"); key != "" {
		n.IdempotencyKey = key
	}
	fields, err := s.applyTemplate(r.Context(), &n, map[uuid.UUID]domain.Template{})
	if err != nil {
		s.writeStoreError(w, r, err)
		return
	}
	if len(fields) > 0 {
		s.writeStoreError(w, r, &domain.ValidationError{Fields: fields})
		return
	}
	n.Normalize()
	if err := n.Validate(); err != nil {
		s.writeStoreError(w, r, err)
		return
	}

	created, isNew, err := s.store.Create(r.Context(), n, logging.CorrelationID(r.Context()), s.maxAttempts)
	if err != nil {
		s.writeStoreError(w, r, err)
		return
	}
	if !isNew { // idempotent replay: same result, no new notification
		writeJSON(w, http.StatusOK, created)
		return
	}
	s.metrics.Created.WithLabelValues(string(created.Channel)).Inc()
	writeJSON(w, http.StatusCreated, created)
}

type batchRequest struct {
	Notifications []domain.NewNotification `json:"notifications"`
}

type batchResponse struct {
	BatchID uuid.UUID   `json:"batch_id"`
	Count   int         `json:"count"`
	IDs     []uuid.UUID `json:"ids"`
}

// createBatch accepts up to 1000 notifications. Validation is all-or-nothing:
// one invalid item rejects the whole batch, so clients never have to work out
// which half of a request went through.
func (s *server) createBatch(w http.ResponseWriter, r *http.Request) {
	var req batchRequest
	if !decodeJSON(w, r, &req) {
		return
	}
	if len(req.Notifications) > 0 && len(req.Notifications) <= domain.MaxBatchSize {
		var fields []domain.FieldError
		cache := map[uuid.UUID]domain.Template{} // each template is loaded once per batch
		for i := range req.Notifications {
			fe, err := s.applyTemplate(r.Context(), &req.Notifications[i], cache)
			if err != nil {
				s.writeStoreError(w, r, err)
				return
			}
			for _, f := range fe {
				fields = append(fields, domain.FieldError{Field: fmt.Sprintf("items[%d].%s", i, f.Field), Message: f.Message})
			}
		}
		if len(fields) > 0 {
			s.writeStoreError(w, r, &domain.ValidationError{Fields: fields})
			return
		}
	}
	for i := range req.Notifications {
		req.Notifications[i].Normalize()
	}
	if err := domain.ValidateBatch(req.Notifications); err != nil {
		s.writeStoreError(w, r, err)
		return
	}

	batchID, created, err := s.store.CreateBatch(r.Context(), req.Notifications, logging.CorrelationID(r.Context()), s.maxAttempts)
	if err != nil {
		s.writeStoreError(w, r, err)
		return
	}
	ids := make([]uuid.UUID, len(created))
	for i, n := range created {
		ids[i] = n.ID
		// An item replaying an idempotency key from an earlier request returns
		// that earlier notification (with its original batch_id); it was not
		// created now, so it is not counted.
		if n.BatchID != nil && *n.BatchID == batchID {
			s.metrics.Created.WithLabelValues(string(n.Channel)).Inc()
		}
	}
	writeJSON(w, http.StatusCreated, batchResponse{BatchID: batchID, Count: len(ids), IDs: ids})
}

// pathID parses the {id} path segment, writing a 400 on failure.
func pathID(w http.ResponseWriter, r *http.Request) (uuid.UUID, bool) {
	id, err := uuid.Parse(r.PathValue("id"))
	if err != nil {
		badRequest(w, "id must be a UUID")
		return uuid.Nil, false
	}
	return id, true
}

func (s *server) getNotification(w http.ResponseWriter, r *http.Request) {
	id, ok := pathID(w, r)
	if !ok {
		return
	}
	n, err := s.store.Get(r.Context(), id)
	if err != nil {
		s.writeStoreError(w, r, err)
		return
	}
	attempts, err := s.store.Attempts(r.Context(), id)
	if err != nil {
		s.writeStoreError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"notification": n, "attempts": attempts})
}

func (s *server) cancelNotification(w http.ResponseWriter, r *http.Request) {
	id, ok := pathID(w, r)
	if !ok {
		return
	}
	n, err := s.store.Cancel(r.Context(), id)
	if err != nil {
		s.writeStoreError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, n)
}

func (s *server) requeueNotification(w http.ResponseWriter, r *http.Request) {
	id, ok := pathID(w, r)
	if !ok {
		return
	}
	n, err := s.store.Requeue(r.Context(), id)
	if err != nil {
		s.writeStoreError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, n)
}

func (s *server) getBatch(w http.ResponseWriter, r *http.Request) {
	id, ok := pathID(w, r)
	if !ok {
		return
	}
	counts, err := s.store.BatchSummary(r.Context(), id)
	if err != nil {
		s.writeStoreError(w, r, err)
		return
	}
	total := 0
	for _, c := range counts {
		total += c
	}
	writeJSON(w, http.StatusOK, map[string]any{"batch_id": id, "total": total, "counts": counts})
}

func (s *server) listNotifications(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	f := store.ListFilter{Limit: defaultPageSize, Cursor: q.Get("cursor")}

	if v := q.Get("limit"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 1 {
			badRequest(w, "limit must be a positive integer")
			return
		}
		f.Limit = min(n, maxPageSize)
	}
	if v := q.Get("status"); v != "" {
		st := domain.Status(v)
		if !st.Valid() {
			badRequest(w, "status must be one of pending, processing, sent, failed, cancelled")
			return
		}
		f.Status = &st
	}
	if v := q.Get("channel"); v != "" {
		ch := domain.Channel(v)
		if !ch.Valid() {
			badRequest(w, "channel must be one of sms, email, push")
			return
		}
		f.Channel = &ch
	}
	if v := q.Get("batch_id"); v != "" {
		id, err := uuid.Parse(v)
		if err != nil {
			badRequest(w, "batch_id must be a UUID")
			return
		}
		f.BatchID = &id
	}
	for _, p := range []struct {
		name string
		dst  **time.Time
	}{{"from", &f.From}, {"to", &f.To}} {
		if v := q.Get(p.name); v != "" {
			t, err := time.Parse(time.RFC3339, v)
			if err != nil {
				badRequest(w, p.name+" must be an RFC3339 timestamp, e.g. 2026-11-18T10:00:00Z")
				return
			}
			*p.dst = &t
		}
	}

	items, next, err := s.store.List(r.Context(), f)
	if err != nil {
		s.writeStoreError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"items": items, "next_cursor": next})
}
