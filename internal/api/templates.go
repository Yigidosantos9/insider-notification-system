package api

import (
	"context"
	"errors"
	"net/http"

	"github.com/google/uuid"

	"github.com/Yigidosantos9/insider-notification-system/internal/domain"
	"github.com/Yigidosantos9/insider-notification-system/internal/store"
)

func (s *server) createTemplate(w http.ResponseWriter, r *http.Request) {
	var t domain.NewTemplate
	if !decodeJSON(w, r, &t) {
		return
	}
	if err := domain.ValidateTemplate(t); err != nil {
		s.writeStoreError(w, r, err)
		return
	}
	created, err := s.store.CreateTemplate(r.Context(), t)
	if err != nil {
		s.writeStoreError(w, r, err)
		return
	}
	writeJSON(w, http.StatusCreated, created)
}

func (s *server) getTemplate(w http.ResponseWriter, r *http.Request) {
	id, ok := pathID(w, r)
	if !ok {
		return
	}
	t, err := s.store.GetTemplate(r.Context(), id)
	if err != nil {
		s.writeStoreError(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, t)
}

// applyTemplate renders n.Content from its template, if it references one.
// It returns field errors for client mistakes and an error only for
// infrastructure failures. The rendered content is then validated like any
// other content (e.g. the 160-character SMS limit).
func (s *server) applyTemplate(ctx context.Context, n *domain.NewNotification, cache map[uuid.UUID]domain.Template) ([]domain.FieldError, error) {
	if n.TemplateID == nil {
		if len(n.Variables) > 0 {
			return []domain.FieldError{{Field: "variables", Message: "only allowed together with template_id"}}, nil
		}
		return nil, nil
	}
	if n.Content != "" {
		return []domain.FieldError{{Field: "content", Message: "must be empty when template_id is set"}}, nil
	}

	tpl, ok := cache[*n.TemplateID]
	if !ok {
		var err error
		tpl, err = s.store.GetTemplate(ctx, *n.TemplateID)
		if errors.Is(err, store.ErrNotFound) {
			return []domain.FieldError{{Field: "template_id", Message: "template not found"}}, nil
		}
		if err != nil {
			return nil, err
		}
		cache[tpl.ID] = tpl
	}

	if n.Channel == "" {
		n.Channel = tpl.Channel
	} else if n.Channel != tpl.Channel {
		return []domain.FieldError{{Field: "channel", Message: "must match the template's channel (" + string(tpl.Channel) + ")"}}, nil
	}
	content, err := domain.RenderTemplate(tpl.Body, n.Variables)
	if err != nil {
		return []domain.FieldError{{Field: "variables", Message: err.Error()}}, nil
	}
	n.Content = content
	return nil, nil
}
