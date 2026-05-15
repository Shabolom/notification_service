package di

import resendNotification "notification_service/internal/resend-notification"

func (d *DI) GetNotificator() *resendNotification.ResendNotification {
	return resendNotification.New(d.GetResend())
}
