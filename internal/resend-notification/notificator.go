package resendNotification

import (
	"log"

	"github.com/resend/resend-go/v2"
)

func (r *ResendNotification) WriteNotification(email string) error {
	params := &resend.SendEmailRequest{
		From:    "onboarding@resend.dev",
		To:      []string{email},
		Subject: "Hello World",
		Html:    "<p>Congrats on sending your <strong>first email</strong>!</p>",
	}

	sent, err := r.client.Emails.Send(params)
	if err != nil {
		log.Fatal("failed to send email: %v", err)
		return err
	}

	log.Printf("email sent: %s", sent.Id)
	return nil
}
