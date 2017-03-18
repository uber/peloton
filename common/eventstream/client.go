package eventstream

import (
	"context"
	"errors"
	log "github.com/Sirupsen/logrus"
	"go.uber.org/yarpc"
	"go.uber.org/yarpc/encoding/json"
	"go.uber.org/yarpc/transport"
	pb_eventstream "peloton/private/eventstream"
	"sync"
	"sync/atomic"
	"time"
)

var (
	// TODO: move these into config, if necessary
	maxEventSize    = 100
	requestTimeout  = 10 * time.Second
	errorRetrySleep = 10 * time.Second
	noEventSleep    = 100 * time.Millisecond
)

// EventHandler is the interface for handling task update events
type EventHandler interface {
	// The event notification callback
	OnEvent(event *pb_eventstream.Event)

	// The events notification callback
	OnEvents(events []*pb_eventstream.Event)

	// Returns the event progress the handler has processed. The value
	// will be used by the client to determine the purgeOffset
	GetEventProgress() uint64
}

// Client is the event stream client
type Client struct {
	sync.RWMutex
	// The rpc client to pull events from event stream handler
	rpcClient json.Client
	// the client name
	clientName string
	// the stream id of the event stream
	streamID string
	// beginOffset of the next pull event request
	beginOffset uint64
	// event purge offset to be send to the event stream handler
	purgeOffset uint64
	// event handler interface to process the received events
	eventHandler EventHandler

	shutdownFlag *int32
	runningState *int32
	started      bool
}

// NewEventStreamClient creates a client that
// consumes from remote event stream handler
func NewEventStreamClient(
	d yarpc.Dispatcher,
	clientName string,
	server string,
	taskUpdateHandler EventHandler) *Client {
	var flag int32
	var runningState int32
	client := &Client{
		clientName:   clientName,
		rpcClient:    json.New(d.ClientConfig(server)),
		shutdownFlag: &flag,
		runningState: &runningState,
		eventHandler: taskUpdateHandler,
	}
	client.Start()
	return client
}

// NewLocalEventStreamClient creates a local client that
// directly consumes from a local event stream handler
func NewLocalEventStreamClient(
	clientName string,
	handler *Handler,
	taskUpdateHandler EventHandler) *Client {
	var flag int32
	var runningState int32

	client := &Client{
		clientName:   clientName,
		rpcClient:    newLocalClient(handler),
		shutdownFlag: &flag,
		runningState: &runningState,
		eventHandler: taskUpdateHandler,
	}
	client.Start()
	return client
}

func newLocalClient(h *Handler) json.Client {
	return &localClient{
		handler: h,
	}
}

// Local client implements json.Client interface. It is a client
// adaptor on an event stream handler, it takes a event stream
// handler and consume event from it. Events from HM->RM would need this.
type localClient struct {
	handler *Handler
}

// Call simply use the underlying handler to handle the request then
// return the response
func (c *localClient) Call(
	ctx context.Context,
	reqMeta yarpc.CallReqMeta,
	reqBody interface{},
	resBodyOut interface{}) (yarpc.CallResMeta, error) {
	initStreamRequest, ok := reqBody.(*pb_eventstream.InitStreamRequest)
	if ok {
		response, _, err := c.handler.InitStream(ctx, nil, initStreamRequest)
		responsePtr, _ := resBodyOut.(*pb_eventstream.InitStreamResponse)
		*responsePtr = *response
		return nil, err
	}

	waitEventsRequest, ok := reqBody.(*pb_eventstream.WaitForEventsRequest)
	if ok {
		response, _, err := c.handler.WaitForEvents(ctx, nil, waitEventsRequest)
		responsePtr, _ := resBodyOut.(*pb_eventstream.WaitForEventsResponse)
		*responsePtr = *response
		resBodyOut = response
		return nil, err
	}
	return nil, errors.New("Unexpected request type")
}

func (c *localClient) CallOneway(
	ctx context.Context,
	reqMeta yarpc.CallReqMeta,
	reqBody interface{}) (transport.Ack, error) {
	return nil, errors.New("Not implemented")
}

func (c *Client) sendInitStreamRequest(clientName string) (*pb_eventstream.InitStreamResponse, error) {
	ctx, cancelFunc := context.WithTimeout(context.Background(), requestTimeout)
	defer cancelFunc()
	request := &pb_eventstream.InitStreamRequest{
		ClientName: clientName,
	}
	var response pb_eventstream.InitStreamResponse
	_, err := c.rpcClient.Call(
		ctx,
		yarpc.NewReqMeta().Procedure("EventStream.InitStream"),
		request,
		&response,
	)
	if err != nil {
		log.WithError(err).Error("sendInitStreamRequest failed")
		return nil, err
	}
	log.WithField("InitStreamResponse", response).Infoln()
	return &response, nil
}

func (c *Client) initStream(clientName string) {
	for c.isRunning() {
		response, err := c.sendInitStreamRequest(c.clientName)
		if err != nil {
			log.WithError(err).Error("sendInitStreamRequest failed")
			time.Sleep(errorRetrySleep)
			continue
		}
		if response.Error != nil {
			log.WithField("InitStreamResponse_Error", response.Error).Error("sendInitStreamRequest failed")
			time.Sleep(errorRetrySleep)
			continue
		}
		c.streamID = response.StreamID
		c.beginOffset = response.PreviousPurgeOffset
		if response.PreviousPurgeOffset < response.MinOffset {
			c.beginOffset = response.MinOffset
		}
		return
	}
	log.Info("initStream returned due to shutdown")
}

func (c *Client) sendWaitEventRequest(
	beginOffset uint64,
	purgeOffset uint64) (*pb_eventstream.WaitForEventsResponse, error) {
	ctx, cancelFunc := context.WithTimeout(context.Background(), requestTimeout)
	defer cancelFunc()

	// We need to make this adjust for the first event,
	// where c.eventHandler.GetEventProgress() and BeginOffset are both 0
	purgeOffset = c.eventHandler.GetEventProgress() + 1
	if purgeOffset > beginOffset {
		purgeOffset = beginOffset
	}

	request := &pb_eventstream.WaitForEventsRequest{
		BeginOffset: beginOffset,
		// purgeOffset are used to move the circular buffer tail, thus plus 1
		PurgeOffset: purgeOffset,
		StreamID:    c.streamID,
		ClientName:  c.clientName,
		Limit:       int32(maxEventSize),
	}
	var response pb_eventstream.WaitForEventsResponse
	_, err := c.rpcClient.Call(
		ctx,
		yarpc.NewReqMeta().Procedure("EventStream.WaitForEvents"),
		request,
		&response,
	)
	if err != nil {
		log.WithError(err).Error("sendWaitForEventsRequest failed")
		return nil, err
	}
	log.WithField("WaitForEventsResponse", response).Debugln()
	return &response, nil
}

func (c *Client) waitEventsLoop() {
	for c.isRunning() {
		c.purgeOffset = c.beginOffset
		response, err := c.sendWaitEventRequest(c.beginOffset, c.purgeOffset)
		// Retry in case there is RPC error
		if err != nil {
			log.WithError(err).Error("sendWaitEventRequest failed")
			time.Sleep(errorRetrySleep)
			continue
		}
		if response.Error != nil {
			log.WithField("waitforEventsError", response.Error).Error("sendWaitEventRequest returns error")
			// If client is unsupported / streamID is invalid, Return and the outside loop should call InitStream
			// again
			if response.Error.GetClientUnsupported() != nil || response.Error.GetInvalidStreamID() != nil {
				return
			}
			// Note: InvalidPurgeOffset should never happen if the client does the right thing. For now, just log it
		}
		log.WithField("Number of events", len(response.Events)).Debug("event received")
		if len(response.Events) == 0 {
			time.Sleep(noEventSleep)
			continue
		}
		if len(response.Events) > 0 {
			for _, event := range response.Events {
				log.WithField("event offset", event.GetOffset()).Debug("Processing event")
				if c.eventHandler != nil {
					c.eventHandler.OnEvent(event)
				}
				c.beginOffset = event.GetOffset() + 1
			}
			c.eventHandler.OnEvents(response.Events)
		}
	}
	log.Info("waitEventsLoop returned due to shutdown")
}

// Start starts the client
func (c *Client) Start() {
	c.Lock()
	defer c.Unlock()
	log.WithField("clientName", c.clientName).Info("Event stream client start() called")
	if !c.started {
		c.started = true
		go func() {
			atomic.StoreInt32(c.runningState, int32(1))
			defer atomic.StoreInt32(c.runningState, int32(0))
			for c.isRunning() {
				c.initStream(c.clientName)
				c.waitEventsLoop()
			}
			log.Info("TaskUpdateStreamClient shutdown")
		}()
		for atomic.LoadInt32(c.runningState) != int32(1) {
			time.Sleep(1 * time.Millisecond)
		}
		log.WithField("clientName", c.clientName).Info("Event stream client started")
	}
}

// Stop stops the client
func (c *Client) Stop() {
	log.WithField("clientName", c.clientName).Info("Event stream client stop() called")
	atomic.StoreInt32(c.shutdownFlag, 1)
	// wait until the evebt consuming go routine returns
	for atomic.LoadInt32(c.runningState) != int32(0) {
		time.Sleep(1 * time.Millisecond)
	}
	log.WithField("clientName", c.clientName).Info("Event stream client stopped")
}

func (c *Client) isRunning() bool {
	return atomic.LoadInt32(c.shutdownFlag) == int32(0)
}
