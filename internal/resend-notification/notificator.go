package resendNotification

import (
	"log"

	"github.com/resend/resend-go/v2"
)

func (r *ResendNotification) WriteNotificationRegister(email string, action string) error {
	params := &resend.SendEmailRequest{
		From:    "onboarding@resend.dev",
		To:      []string{email},
		Subject: action,
		Html: `
		<h2>Welcome!</h2>

		<p>
			Your account has been successfully created.
		</p>

		<p>
			You can now sign in and start using the platform.
		</p>

		<hr>

		<p>
			If you did not create this account, please contact support immediately.
		</p>
	`,
	}

	sent, err := r.client.Emails.Send(params)
	if err != nil {
		return err
	}

	log.Printf("email sent: %s", sent.Id)
	return nil
}

func (r *ResendNotification) WriteNotificationLogin(email string, action string) error {
	params := &resend.SendEmailRequest{
		From:    "onboarding@resend.dev",
		To:      []string{email},
		Subject: action,
		Html: `
		<h2>Login Successful</h2>

		<p>
			Your account was successfully signed in.
		</p>

		<p>
			If this was you, no further action is required.
		</p>

		<hr>

		<p>
			If you do not recognize this activity, we strongly recommend changing your password immediately.
		</p>
	`,
	}

	sent, err := r.client.Emails.Send(params)
	if err != nil {
		return err
	}

	log.Printf("email sent: %s", sent.Id)
	return nil
}
