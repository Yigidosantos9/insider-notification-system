package store

import (
	"context"
	"errors"
	"testing"

	"github.com/google/uuid"

	"github.com/Yigidosantos9/insider-notification-system/internal/domain"
)

func TestTemplates(t *testing.T) {
	s := newStore(t)
	ctx := context.Background()
	in := domain.NewTemplate{Name: "otp", Channel: domain.ChannelSMS, Body: "Code: {{.code}}"}

	created, err := s.CreateTemplate(ctx, in)
	if err != nil || created.ID == uuid.Nil || created.Name != "otp" {
		t.Fatalf("create: %+v %v", created, err)
	}
	got, err := s.GetTemplate(ctx, created.ID)
	if err != nil || got.Body != in.Body || got.Channel != domain.ChannelSMS {
		t.Fatalf("get: %+v %v", got, err)
	}
	if _, err := s.CreateTemplate(ctx, in); !errors.Is(err, ErrTemplateNameTaken) {
		t.Fatalf("duplicate name: %v", err)
	}
	if _, err := s.GetTemplate(ctx, uuid.New()); !errors.Is(err, ErrNotFound) {
		t.Fatalf("missing: %v", err)
	}
}
