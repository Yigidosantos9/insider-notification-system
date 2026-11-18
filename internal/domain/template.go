package domain

import (
	"strings"
	"text/template"
	"time"

	"github.com/google/uuid"
)

// NewTemplate is a request to store a reusable message template.
// Body uses Go template syntax: "Hi {{.name}}, your code is {{.code}}".
type NewTemplate struct {
	Name    string  `json:"name"`
	Channel Channel `json:"channel"`
	Body    string  `json:"body"`
}

// Template is a stored message template.
type Template struct {
	ID        uuid.UUID `json:"id"`
	Name      string    `json:"name"`
	Channel   Channel   `json:"channel"`
	Body      string    `json:"body"`
	CreatedAt time.Time `json:"created_at"`
}

// ValidateTemplate checks required fields and that the body parses.
func ValidateTemplate(t NewTemplate) error {
	ve := &ValidationError{}
	if strings.TrimSpace(t.Name) == "" || len(t.Name) > 100 {
		ve.add("name", "is required (max 100 characters)")
	}
	if !t.Channel.Valid() {
		ve.add("channel", "must be one of sms, email, push")
	}
	if strings.TrimSpace(t.Body) == "" {
		ve.add("body", "is required")
	} else if _, err := parse(t.Body); err != nil {
		ve.add("body", "invalid template: "+err.Error())
	}
	return ve.orNil()
}

// RenderTemplate substitutes variables into body. A variable referenced by the
// template but missing from vars is an error, never an empty string — sending
// "Hi , your code is " to a customer is worse than rejecting the request.
func RenderTemplate(body string, vars map[string]string) (string, error) {
	tpl, err := parse(body)
	if err != nil {
		return "", err
	}
	if vars == nil {
		vars = map[string]string{}
	}
	var sb strings.Builder
	if err := tpl.Execute(&sb, vars); err != nil {
		return "", err
	}
	return sb.String(), nil
}

func parse(body string) (*template.Template, error) {
	return template.New("t").Option("missingkey=error").Parse(body)
}
