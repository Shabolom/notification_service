package resendNotification

import "github.com/resend/resend-go/v2"

type ResendNotification struct {
	client *resend.Client
}

func New(client *resend.Client) *ResendNotification {
	return &ResendNotification{
		client: client,
	}
}
