package handler

import (
	"context"

	sqsclient "github.com/inaciogu/go-sqs/consumer"
)

// SQSHandler is responsible for running the SQS clients concurrently
type SQSHandler struct {
	Clients []sqsclient.SQSClientInterface
}

func New(clients []sqsclient.SQSClientInterface) *SQSHandler {
	return &SQSHandler{
		Clients: clients,
	}
}

func (h *SQSHandler) Run(ctx context.Context) {
	for _, client := range h.Clients {
		go client.Start(ctx)
	}

	select {}
}
