package iface

import "context"

type Consumer[T any] interface {
	Consume(context.Context, func(context.Context, []*T) error)
	Close() error
	SubscribeTopic(string) error
}
