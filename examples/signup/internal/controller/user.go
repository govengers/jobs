// Package controller is the example's transport layer: the HTTP routes and the job handlers
// translate their transport and call the service (ARC-001).
package controller

import (
	"encoding/json"
	"errors"
	"net/http"

	"github.com/govengers/jobs/examples/signup/internal/model"
	"github.com/govengers/jobs/examples/signup/internal/service"
)

// maximumBodyBytes bounds a sign-up request.
const maximumBodyBytes = 64 << 10

// UserController serves POST /api/users.
type UserController struct {
	service *service.UserService
}

// UserControllerOptions configures a UserController.
type UserControllerOptions struct {
	UserService *service.UserService // required
}

// NewUserController returns a UserController.
func NewUserController(options *UserControllerOptions) (*UserController, error) {
	if options == nil || options.UserService == nil {
		return nil, errors.New("controller: UserControllerOptions.UserService is required")
	}
	return &UserController{service: options.UserService}, nil
}

// Register mounts the routes.
func (c *UserController) Register(router *http.ServeMux) {
	router.HandleFunc("POST /api/users", c.signUp)
}

func (c *UserController) signUp(writer http.ResponseWriter, request *http.Request) {
	input := &model.SignUpInput{}
	decoder := json.NewDecoder(http.MaxBytesReader(writer, request.Body, maximumBodyBytes))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(input); err != nil {
		respond(writer, http.StatusBadRequest, map[string]string{"error": "invalid_request"})
		return
	}
	user, err := c.service.SignUp(request.Context(), input)
	switch {
	case errors.Is(err, model.ErrInvalidInput):
		respond(writer, http.StatusBadRequest, map[string]string{"error": "invalid_input"})
	case errors.Is(err, model.ErrEmailTaken):
		respond(writer, http.StatusConflict, map[string]string{"error": "email_taken"})
	case err != nil:
		respond(writer, http.StatusInternalServerError, map[string]string{"error": "internal"})
	default:
		respond(writer, http.StatusCreated, user)
	}
}

func respond(writer http.ResponseWriter, status int, body any) {
	writer.Header().Set("Content-Type", "application/json")
	writer.WriteHeader(status)
	_ = json.NewEncoder(writer).Encode(body) // the client went away; nothing left to tell it
}
