package service

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/govengers/jobs/examples/signup/internal/model"
)

func TestValidateSignUp(t *testing.T) {
	tests := []struct {
		name    string
		input   model.SignUpInput
		want    model.SignUpInput
		invalid bool
	}{
		{name: "normalizes", input: model.SignUpInput{Email: " Ada@Example.COM ", Name: " Ada "}, want: model.SignUpInput{Email: "ada@example.com", Name: "Ada"}},
		{name: "display name form", input: model.SignUpInput{Email: "Ada <ada@example.com>", Name: "Ada"}, invalid: true},
		{name: "no address", input: model.SignUpInput{Email: "ada", Name: "Ada"}, invalid: true},
		{name: "no name", input: model.SignUpInput{Email: "ada@example.com", Name: "  "}, invalid: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			normalized, err := validateSignUp(&test.input)

			if test.invalid {
				require.ErrorIs(t, err, model.ErrInvalidInput)
				return
			}
			require.NoError(t, err)
			require.Equal(t, test.want, *normalized)
		})
	}
}
