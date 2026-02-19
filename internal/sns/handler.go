package sns

import (
	"encoding/json"
	"encoding/xml"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"strings"

	"github.com/yarlson/mokapot/internal/query"
)

// Handler dispatches SNS actions to the engine.
type Handler struct {
	engine *Engine
}

// NewHandler creates a new SNS action handler.
func NewHandler(engine *Engine) *Handler {
	return &Handler{engine: engine}
}

// HandleRequest dispatches an SNS action, detecting protocol from Content-Type.
func (h *Handler) HandleRequest(w http.ResponseWriter, r *http.Request) {
	ct := r.Header.Get("Content-Type")
	if strings.Contains(ct, "application/x-amz-json") {
		h.handleJSON(w, r)
	} else {
		h.handleQuery(w, r)
	}
}

// --- JSON protocol (AWS JSON 1.0) ---

func (h *Handler) handleJSON(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, 1<<20))
	if err != nil {
		writeJSONError(w, http.StatusBadRequest, "InvalidParameter", "Failed to read request body.")
		return
	}

	var raw map[string]json.RawMessage
	if err := json.Unmarshal(body, &raw); err != nil {
		writeJSONError(w, http.StatusBadRequest, "InvalidParameter", "Failed to parse JSON body.")
		return
	}

	target := r.Header.Get("X-Amz-Target")
	action := target
	if idx := strings.LastIndex(target, "."); idx >= 0 {
		action = target[idx+1:]
	}

	switch action {
	case "CreateTopic":
		h.createTopicJSON(w, raw)
	case "Subscribe":
		h.subscribeJSON(w, raw)
	case "Publish":
		h.publishJSON(w, raw)
	default:
		if IsSNSAction(action) {
			writeJSONError(w, http.StatusBadRequest, "InvalidAction", "The action "+action+" is not yet implemented.")
		} else {
			writeJSONError(w, http.StatusBadRequest, "InvalidAction", "The action "+action+" is not valid for this endpoint.")
		}
	}
}

func jsonString(raw map[string]json.RawMessage, key string) string {
	v, ok := raw[key]
	if !ok {
		return ""
	}
	var s string
	if err := json.Unmarshal(v, &s); err != nil {
		return ""
	}
	return s
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/x-amz-json-1.0")
	w.WriteHeader(http.StatusOK)
	if err := json.NewEncoder(w).Encode(v); err != nil {
		slog.Warn("failed to encode JSON response", "err", err)
	}
}

func writeJSONError(w http.ResponseWriter, status int, code, message string) {
	w.Header().Set("Content-Type", "application/x-amz-json-1.0")
	w.Header().Set("x-amzn-query-error", code+";Sender")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(map[string]string{
		"__type":  code,
		"Message": message,
	}); err != nil {
		slog.Warn("failed to encode JSON error response", "err", err)
	}
}

func writeSNSJSONError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, ErrTopicNotFound):
		writeJSONError(w, http.StatusNotFound, "NotFound", "Topic does not exist.")
	case errors.Is(err, ErrInvalidParameter):
		writeJSONError(w, http.StatusBadRequest, "InvalidParameter", sanitizeErrorMessage(err))
	case errors.Is(err, ErrInvalidParameterValue):
		writeJSONError(w, http.StatusBadRequest, "InvalidParameterValue", sanitizeErrorMessage(err))
	default:
		writeJSONError(w, http.StatusInternalServerError, "InternalError", "An internal error occurred.")
	}
}

func sanitizeErrorMessage(err error) string {
	msg := err.Error()
	if idx := strings.Index(msg, ": "); idx >= 0 {
		return msg[idx+2:]
	}
	return msg
}

func (h *Handler) createTopicJSON(w http.ResponseWriter, raw map[string]json.RawMessage) {
	name := jsonString(raw, "Name")
	if name == "" {
		writeJSONError(w, http.StatusBadRequest, "InvalidParameter", "Topic name is required.")
		return
	}

	topic, err := h.engine.CreateTopic(name)
	if err != nil {
		writeSNSJSONError(w, err)
		return
	}

	writeJSON(w, map[string]string{"TopicArn": topic.ARN})
}

func (h *Handler) subscribeJSON(w http.ResponseWriter, raw map[string]json.RawMessage) {
	topicARN := jsonString(raw, "TopicArn")
	protocol := jsonString(raw, "Protocol")
	endpoint := jsonString(raw, "Endpoint")

	if topicARN == "" {
		writeJSONError(w, http.StatusBadRequest, "InvalidParameter", "TopicArn is required.")
		return
	}

	sub, err := h.engine.Subscribe(topicARN, protocol, endpoint)
	if err != nil {
		writeSNSJSONError(w, err)
		return
	}

	writeJSON(w, map[string]string{"SubscriptionArn": sub.SubscriptionARN})
}

func (h *Handler) publishJSON(w http.ResponseWriter, raw map[string]json.RawMessage) {
	topicARN := jsonString(raw, "TopicArn")
	message := jsonString(raw, "Message")
	subject := jsonString(raw, "Subject")

	if topicARN == "" {
		writeJSONError(w, http.StatusBadRequest, "InvalidParameter", "TopicArn is required.")
		return
	}

	result, err := h.engine.Publish(topicARN, message, subject)
	if err != nil {
		writeSNSJSONError(w, err)
		return
	}

	writeJSON(w, map[string]string{"MessageId": result.MessageID})
}

// --- Query protocol (form-encoded + XML) ---

func (h *Handler) handleQuery(w http.ResponseWriter, r *http.Request) {
	params, err := query.ParseRequest(r)
	if err != nil {
		query.WriteError(w, http.StatusBadRequest, "InvalidParameter", "Failed to parse request body.")
		return
	}

	action := params.Action()

	switch action {
	case "CreateTopic":
		h.createTopicXML(w, params)
	case "Subscribe":
		h.subscribeXML(w, params)
	case "Publish":
		h.publishXML(w, params)
	default:
		if IsSNSAction(action) {
			query.WriteError(w, http.StatusBadRequest, "InvalidAction", "The action "+action+" is not yet implemented.")
		} else {
			query.WriteError(w, http.StatusBadRequest, "InvalidAction", "The action "+action+" is not valid for this endpoint.")
		}
	}
}

// --- XML types ---

type createTopicXMLResponse struct {
	XMLName  xml.Name             `xml:"CreateTopicResponse"`
	Result   createTopicXMLResult `xml:"CreateTopicResult"`
	Metadata query.ResponseMetadata
}

type createTopicXMLResult struct {
	TopicARN string `xml:"TopicArn"`
}

func (h *Handler) createTopicXML(w http.ResponseWriter, params query.Params) {
	name := params.Get("Name")
	if name == "" {
		query.WriteError(w, http.StatusBadRequest, "InvalidParameter", "Topic name is required.")
		return
	}

	topic, err := h.engine.CreateTopic(name)
	if err != nil {
		writeSNSErrorXML(w, err)
		return
	}

	query.WriteXML(w, http.StatusOK, createTopicXMLResponse{
		Result:   createTopicXMLResult{TopicARN: topic.ARN},
		Metadata: query.ResponseMetadata{RequestID: query.NewRequestID()},
	})
}

type subscribeXMLResponse struct {
	XMLName  xml.Name           `xml:"SubscribeResponse"`
	Result   subscribeXMLResult `xml:"SubscribeResult"`
	Metadata query.ResponseMetadata
}

type subscribeXMLResult struct {
	SubscriptionARN string `xml:"SubscriptionArn"`
}

func (h *Handler) subscribeXML(w http.ResponseWriter, params query.Params) {
	topicARN := params.Get("TopicArn")
	protocol := params.Get("Protocol")
	endpoint := params.Get("Endpoint")

	if topicARN == "" {
		query.WriteError(w, http.StatusBadRequest, "InvalidParameter", "TopicArn is required.")
		return
	}

	sub, err := h.engine.Subscribe(topicARN, protocol, endpoint)
	if err != nil {
		writeSNSErrorXML(w, err)
		return
	}

	query.WriteXML(w, http.StatusOK, subscribeXMLResponse{
		Result:   subscribeXMLResult{SubscriptionARN: sub.SubscriptionARN},
		Metadata: query.ResponseMetadata{RequestID: query.NewRequestID()},
	})
}

type publishXMLResponse struct {
	XMLName  xml.Name         `xml:"PublishResponse"`
	Result   publishXMLResult `xml:"PublishResult"`
	Metadata query.ResponseMetadata
}

type publishXMLResult struct {
	MessageID string `xml:"MessageId"`
}

func (h *Handler) publishXML(w http.ResponseWriter, params query.Params) {
	topicARN := params.Get("TopicArn")
	message := params.Get("Message")
	subject := params.Get("Subject")

	if topicARN == "" {
		query.WriteError(w, http.StatusBadRequest, "InvalidParameter", "TopicArn is required.")
		return
	}

	result, err := h.engine.Publish(topicARN, message, subject)
	if err != nil {
		writeSNSErrorXML(w, err)
		return
	}

	query.WriteXML(w, http.StatusOK, publishXMLResponse{
		Result:   publishXMLResult{MessageID: result.MessageID},
		Metadata: query.ResponseMetadata{RequestID: query.NewRequestID()},
	})
}

func writeSNSErrorXML(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, ErrTopicNotFound):
		query.WriteError(w, http.StatusNotFound, "NotFound", "Topic does not exist.")
	case errors.Is(err, ErrInvalidParameter):
		query.WriteError(w, http.StatusBadRequest, "InvalidParameter", sanitizeErrorMessage(err))
	case errors.Is(err, ErrInvalidParameterValue):
		query.WriteError(w, http.StatusBadRequest, "InvalidParameterValue", sanitizeErrorMessage(err))
	default:
		query.WriteError(w, http.StatusInternalServerError, "InternalError", "An internal error occurred.")
	}
}

// IsSNSAction returns true if the given action name is an SNS action.
func IsSNSAction(action string) bool {
	switch action {
	case "CreateTopic", "DeleteTopic", "ListTopics",
		"GetTopicAttributes", "SetTopicAttributes",
		"Subscribe", "Unsubscribe", "ListSubscriptionsByTopic",
		"GetSubscriptionAttributes", "SetSubscriptionAttributes",
		"Publish":
		return true
	}
	return false
}
