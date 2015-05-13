package notifications

import (
	"sync"

	"golang.org/x/net/context"
	grpc "google.golang.org/grpc"

	grpcClient "github.com/docker/distribution/notifications/grpc"
)

type grpcSink struct {
	url string

	mu     sync.Mutex
	closed bool
	client grpcClient.EventReceiverClient
	conn   *grpc.ClientConn
}

func newGrpcSink(url string) *grpcSink {
	conn, err := grpc.Dial(url)
	if err != nil {
		return nil
	}
	client := grpcClient.NewEventReceiverClient(conn)
	return &grpcSink{
		url:    url,
		client: client,
		conn:   conn,
	}
}

func (g *grpcSink) Write(e ...Event) error {
	events := make([]*grpcClient.Event, 0, len(e))
	for _, ev := range e {
		events = append(events, &grpcClient.Event{
			ID:        ev.ID,
			Timestamp: ev.Timestamp.Unix(),
			Action:    ev.Action,
			Target: &grpcClient.TargetMessage{
				Repository: ev.Target.Repository,
				URL:        ev.Target.URL,
				Descriptor_: &grpcClient.DescriptorMessage{
					MediaType: ev.Target.Descriptor.MediaType,
					Length:    ev.Target.Descriptor.Length,
					Digest:    ev.Target.Descriptor.Digest.String(),
				},
			},
			Request: &grpcClient.RequestRecordMessage{
				ID:        ev.Request.ID,
				Addr:      ev.Request.Addr,
				Host:      ev.Request.Host,
				Method:    ev.Request.Method,
				UserAgent: ev.Request.UserAgent,
			},
			Actor: &grpcClient.ActorRecordMessage{
				Name: ev.Actor.Name,
			},
			Source: &grpcClient.SourceRecordMessage{
				Addr:       ev.Source.Addr,
				InstanceID: ev.Source.InstanceID,
			},
		})
	}
	_, err := g.client.Publish(context.Background(), &grpcClient.Events{Events: events})
	return err
}

func (g *grpcSink) Close() error {
	return g.conn.Close()
}
