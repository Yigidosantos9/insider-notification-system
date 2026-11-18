package domain

import (
	"strings"
	"testing"
)

func TestRenderTemplate(t *testing.T) {
	got, err := RenderTemplate("Hi {{.name}}, your code is {{.code}}", map[string]string{"name": "Ayşe", "code": "1234"})
	if err != nil || got != "Hi Ayşe, your code is 1234" {
		t.Fatalf("got %q, %v", got, err)
	}
}

func TestRenderTemplateMissingVariable(t *testing.T) {
	_, err := RenderTemplate("Hi {{.name}}", map[string]string{})
	if err == nil || !strings.Contains(err.Error(), "name") {
		t.Fatalf("expected missing variable error naming 'name', got %v", err)
	}
}

func TestValidateTemplate(t *testing.T) {
	if err := ValidateTemplate(NewTemplate{Name: "otp", Channel: ChannelSMS, Body: "Code: {{.code}}"}); err != nil {
		t.Fatalf("valid template rejected: %v", err)
	}
	cases := map[string]NewTemplate{
		"body":    {Name: "x", Channel: ChannelSMS, Body: "{{.unclosed"},
		"name":    {Name: "", Channel: ChannelSMS, Body: "hi"},
		"channel": {Name: "x", Channel: "fax", Body: "hi"},
	}
	for field, tpl := range cases {
		got := fieldsOf(t, ValidateTemplate(tpl))
		if len(got) != 1 || got[0] != field {
			t.Fatalf("%s: got %v", field, got)
		}
	}
}
