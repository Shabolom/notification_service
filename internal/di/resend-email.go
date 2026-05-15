package di

import "github.com/resend/resend-go/v2"

func (d *DI) GetResend() *resend.Client {
	if d.notificator != nil {
		return d.notificator
	}

	client := resend.NewClient(d.Config().ResendAppKey)
	d.notificator = client

	return client
}
