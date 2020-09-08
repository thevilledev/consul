package subscribe

import (
	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/go-uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/hashicorp/consul/acl"
	"github.com/hashicorp/consul/agent/consul/stream"
	"github.com/hashicorp/consul/proto/pbsubscribe"
)

// Server implements a StateChangeSubscriptionServer for accepting SubscribeRequests,
// and sending events to the subscription topic.
type Server struct {
	backend Backend
	logger  hclog.Logger
	cfg     Config
}

var _ pbsubscribe.StateChangeSubscriptionServer = (*Server)(nil)

type Backend interface {
	ResolveToken(token string) (acl.Authorizer, error)
	GRPCConn(dc string) (*grpc.ClientConn, error)
	Subscribe(req *stream.SubscribeRequest) (*stream.Subscription, error)
}

type Config struct {
	Datacenter string
}

func (h *Server) Subscribe(req *pbsubscribe.SubscribeRequest, serverStream pbsubscribe.StateChangeSubscription_SubscribeServer) error {
	// streamID is just used for message correlation in trace logs and not
	// populated normally.
	var streamID string

	if h.logger.IsTrace() {
		// TODO(banks) it might be nice one day to replace this with OpenTracing ID
		// if one is set etc. but probably pointless until we support that properly
		// in other places so it's actually propagated properly. For now this just
		// makes lifetime of a stream more traceable in our regular server logs for
		// debugging/dev.
		var err error
		streamID, err = uuid.GenerateUUID()
		if err != nil {
			return err
		}
	}

	if req.Datacenter != "" && req.Datacenter != h.cfg.Datacenter {
		return h.forwardAndProxy(req, serverStream, streamID)
	}

	h.logger.Trace("new subscription",
		"topic", req.Topic.String(),
		"key", req.Key,
		"index", req.Index,
		"stream_id", streamID,
	)

	var sentCount uint64
	defer h.logger.Trace("subscription closed", "stream_id", streamID)

	// Resolve the token and create the ACL filter.
	// TODO: handle token expiry gracefully...
	authz, err := h.backend.ResolveToken(req.Token)
	if err != nil {
		return err
	}

	sub, err := h.backend.Subscribe(toStreamSubscribeRequest(req))
	if err != nil {
		return err
	}
	defer sub.Unsubscribe()

	ctx := serverStream.Context()
	snapshotDone := false
	for {
		events, err := sub.Next(ctx)
		switch {
		case err == stream.ErrSubscriptionClosed:
			h.logger.Trace("subscription reset by server", "stream_id", streamID)
			return status.Error(codes.Aborted, err.Error())

		case err != nil:
			return err
		}

		events = filterStreamEvents(authz, events)
		if len(events) == 0 {
			continue
		}

		first := events[0]
		switch {
		case first.IsEndOfSnapshot() || first.IsEndOfEmptySnapshot():
			snapshotDone = true
			h.logger.Trace("snapshot complete",
				"index", first.Index, "sent", sentCount, "stream_id", streamID)
		case snapshotDone:
			h.logger.Trace("sending events",
				"index", first.Index,
				"sent", sentCount,
				"batch_size", len(events),
				"stream_id", streamID,
			)
		}

		e := newEventFromStreamEvents(req, events)
		sentCount += uint64(len(events))
		if err := serverStream.Send(e); err != nil {
			return err
		}
	}
}

// TODO: can be replaced by mog conversion
func toStreamSubscribeRequest(req *pbsubscribe.SubscribeRequest) *stream.SubscribeRequest {
	return &stream.SubscribeRequest{
		// TODO: translate topic or use protobuf topic in state package
		Topic: req.Topic,
		Key:   req.Key,
		Token: req.Token,
		Index: req.Index,
	}
}

func (h *Server) forwardAndProxy(
	req *pbsubscribe.SubscribeRequest,
	serverStream pbsubscribe.StateChangeSubscription_SubscribeServer,
	streamID string,
) error {
	conn, err := h.backend.GRPCConn(req.Datacenter)
	if err != nil {
		return err
	}

	h.logger.Trace("forwarding to another DC",
		"dc", req.Datacenter,
		"topic", req.Topic.String(),
		"key", req.Key,
		"index", req.Index,
		"stream_id", streamID,
	)

	defer func() {
		h.logger.Trace("forwarded stream closed",
			"dc", req.Datacenter,
			"stream_id", streamID,
		)
	}()

	client := pbsubscribe.NewStateChangeSubscriptionClient(conn)
	streamHandle, err := client.Subscribe(serverStream.Context(), req)
	if err != nil {
		return err
	}

	for {
		event, err := streamHandle.Recv()
		if err != nil {
			return err
		}
		if err := serverStream.Send(event); err != nil {
			return err
		}
	}
}

// filterStreamEvents to only those allowed by the acl token.
func filterStreamEvents(authz acl.Authorizer, events []stream.Event) []stream.Event {
	// TODO: when is authz nil?
	if authz == nil || len(events) == 0 {
		return events
	}

	// Fast path for the common case of only 1 event since we can avoid slice
	// allocation in the hot path of every single update event delivered in vast
	// majority of cases with this. Note that this is called _per event/item_ when
	// sending snapshots which is a lot worse than being called once on regular
	// result.
	if len(events) == 1 {
		if enforceACL(authz, events[0]) == acl.Allow {
			return events
		}
		return nil
	}

	var filtered []stream.Event

	for idx := range events {
		event := events[idx]
		if enforceACL(authz, event) == acl.Allow {
			filtered = append(filtered, event)
		}
	}

	return filtered
}

func newEventFromStreamEvents(req *pbsubscribe.SubscribeRequest, events []stream.Event) *pbsubscribe.Event {
	e := &pbsubscribe.Event{
		Topic: req.Topic,
		Key:   req.Key,
		Index: events[0].Index,
	}
	if len(events) == 1 {
		setPayload(e, events[0].Payload)
		return e
	}

	e.Payload = &pbsubscribe.Event_EventBatch{
		EventBatch: &pbsubscribe.EventBatch{
			Events: batchEventsFromEventSlice(events),
		},
	}
	return e
}

// TODO:
func setPayload(e *pbsubscribe.Event, payload interface{}) {
	switch payload.(type) {
	}
	panic("event payload not implemented")
}

func batchEventsFromEventSlice(events []stream.Event) []*pbsubscribe.Event {
	ret := make([]*pbsubscribe.Event, len(events))
	for i := range events {
		ret[i] = &pbsubscribe.Event{}
		setPayload(ret[i], events[i].Payload)
	}
	return ret
}
