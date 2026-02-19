package sqs

import (
	"encoding/json"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"sort"
	"strconv"
	"strings"

	"github.com/yarlson/mokapot/internal/query"
)

// Handler dispatches SQS actions to the engine.
type Handler struct {
	engine *Engine
}

// NewHandler creates a new SQS action handler.
func NewHandler(engine *Engine) *Handler {
	return &Handler{engine: engine}
}

// HandleRequest dispatches an SQS action, detecting protocol from Content-Type.
func (h *Handler) HandleRequest(w http.ResponseWriter, r *http.Request, pathQueueName string) {
	ct := r.Header.Get("Content-Type")
	if strings.Contains(ct, "application/x-amz-json") {
		h.handleJSON(w, r, pathQueueName)
	} else {
		h.handleQuery(w, r, pathQueueName)
	}
}

// --- JSON protocol (AWS JSON 1.0, used by Go/JS SDK v3) ---

func (h *Handler) handleJSON(w http.ResponseWriter, r *http.Request, pathQueueName string) {
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, 1<<20))
	if err != nil {
		writeJSONError(w, http.StatusBadRequest, "InvalidParameterValue", "Failed to read request body.")
		return
	}

	var raw map[string]json.RawMessage
	if err := json.Unmarshal(body, &raw); err != nil {
		writeJSONError(w, http.StatusBadRequest, "InvalidParameterValue", "Failed to parse JSON body.")
		return
	}

	// Action from X-Amz-Target: "AmazonSQS.CreateQueue"
	target := r.Header.Get("X-Amz-Target")
	action := target
	if idx := strings.LastIndex(target, "."); idx >= 0 {
		action = target[idx+1:]
	}

	switch action {
	case "CreateQueue":
		h.createQueueJSON(w, raw)
	case "GetQueueUrl":
		h.getQueueURLJSON(w, raw)
	case "GetQueueAttributes":
		h.getQueueAttributesJSON(w, raw, pathQueueName)
	case "SetQueueAttributes":
		h.setQueueAttributesJSON(w, raw, pathQueueName)
	case "SendMessage":
		h.sendMessageJSON(w, raw, pathQueueName)
	case "ReceiveMessage":
		h.receiveMessageJSON(w, r, raw, pathQueueName)
	case "DeleteMessage":
		h.deleteMessageJSON(w, raw, pathQueueName)
	case "SendMessageBatch":
		h.sendMessageBatchJSON(w, raw, pathQueueName)
	case "DeleteMessageBatch":
		h.deleteMessageBatchJSON(w, raw, pathQueueName)
	case "PurgeQueue":
		h.purgeQueueJSON(w, raw, pathQueueName)
	default:
		writeJSONError(w, http.StatusBadRequest, "InvalidAction", "The action "+action+" is not valid for this endpoint.")
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

func jsonInt(raw map[string]json.RawMessage, key string) (int, bool) {
	v, ok := raw[key]
	if !ok {
		return 0, false
	}
	var n int
	if err := json.Unmarshal(v, &n); err != nil {
		return 0, false
	}
	return n, true
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

func writeJSONQueueError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, ErrQueueDoesNotExist):
		writeJSONError(w, http.StatusBadRequest, "AWS.SimpleQueueService.NonExistentQueue", "The specified queue does not exist.")
	case errors.Is(err, ErrReceiptHandleIsInvalid):
		writeJSONError(w, http.StatusBadRequest, "ReceiptHandleIsInvalid", "The input receipt handle is invalid.")
	case errors.Is(err, ErrEmptyBatchRequest):
		writeJSONError(w, http.StatusBadRequest, "AWS.SimpleQueueService.EmptyBatchRequest", sanitizeErrorMessage(err))
	case errors.Is(err, ErrTooManyEntriesInBatchRequest):
		writeJSONError(w, http.StatusBadRequest, "AWS.SimpleQueueService.TooManyEntriesInBatchRequest", sanitizeErrorMessage(err))
	case errors.Is(err, ErrBatchEntryIdsNotDistinct):
		writeJSONError(w, http.StatusBadRequest, "AWS.SimpleQueueService.BatchEntryIdsNotDistinct", sanitizeErrorMessage(err))
	case errors.Is(err, ErrPurgeQueueInProgress):
		writeJSONError(w, http.StatusForbidden, "AWS.SimpleQueueService.PurgeQueueInProgress", "Only one PurgeQueue operation is allowed every 60 seconds.")
	case errors.Is(err, ErrInvalidParameterValue):
		writeJSONError(w, http.StatusBadRequest, "InvalidParameterValue", sanitizeErrorMessage(err))
	default:
		writeJSONError(w, http.StatusInternalServerError, "InternalError", "An internal error occurred.")
	}
}

// sanitizeErrorMessage strips the sentinel error prefix from wrapped errors,
// returning only the descriptive suffix. This avoids leaking internal error
// chains (e.g. JSON parse details) to API callers.
func sanitizeErrorMessage(err error) string {
	msg := err.Error()
	// Wrapped errors look like "InvalidParameterValue: actual message".
	// Strip the sentinel prefix to return just the meaningful part.
	if idx := strings.Index(msg, ": "); idx >= 0 {
		return msg[idx+2:]
	}
	return msg
}

func (h *Handler) createQueueJSON(w http.ResponseWriter, raw map[string]json.RawMessage) {
	name := jsonString(raw, "QueueName")
	if name == "" {
		writeJSONError(w, http.StatusBadRequest, "InvalidParameterValue", "Value for parameter QueueName is invalid. Reason: Must specify a queue name.")
		return
	}

	q, err := h.engine.CreateQueue(name)
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, "InternalError", "An internal error occurred.")
		return
	}

	writeJSON(w, map[string]string{"QueueUrl": q.URL})
}

func (h *Handler) getQueueURLJSON(w http.ResponseWriter, raw map[string]json.RawMessage) {
	name := jsonString(raw, "QueueName")
	if name == "" {
		writeJSONError(w, http.StatusBadRequest, "InvalidParameterValue", "Value for parameter QueueName is invalid. Reason: Must specify a queue name.")
		return
	}

	url, err := h.engine.GetQueueURL(name)
	if err != nil {
		writeJSONQueueError(w, err)
		return
	}

	writeJSON(w, map[string]string{"QueueUrl": url})
}

func jsonStringSlice(raw map[string]json.RawMessage, key string) []string {
	v, ok := raw[key]
	if !ok {
		return nil
	}
	var s []string
	if err := json.Unmarshal(v, &s); err != nil {
		return nil
	}
	return s
}

func jsonStringMap(raw map[string]json.RawMessage, key string) map[string]string {
	v, ok := raw[key]
	if !ok {
		return nil
	}
	var m map[string]string
	if err := json.Unmarshal(v, &m); err != nil {
		return nil
	}
	return m
}

func (h *Handler) getQueueAttributesJSON(w http.ResponseWriter, raw map[string]json.RawMessage, pathQueueName string) {
	queueName := pathQueueName
	if queueName == "" {
		queueName = queueNameFromURL(jsonString(raw, "QueueUrl"))
	}
	if queueName == "" {
		writeJSONError(w, http.StatusBadRequest, "InvalidParameterValue", "Queue name is required.")
		return
	}

	attrNames := jsonStringSlice(raw, "AttributeNames")
	if attrNames == nil {
		attrNames = []string{"All"}
	}

	attrs, err := h.engine.GetQueueAttributes(queueName, attrNames)
	if err != nil {
		writeJSONQueueError(w, err)
		return
	}

	writeJSON(w, map[string]any{"Attributes": attrs})
}

func (h *Handler) setQueueAttributesJSON(w http.ResponseWriter, raw map[string]json.RawMessage, pathQueueName string) {
	queueName := pathQueueName
	if queueName == "" {
		queueName = queueNameFromURL(jsonString(raw, "QueueUrl"))
	}
	if queueName == "" {
		writeJSONError(w, http.StatusBadRequest, "InvalidParameterValue", "Queue name is required.")
		return
	}

	attrs := jsonStringMap(raw, "Attributes")
	if len(attrs) == 0 {
		writeJSONError(w, http.StatusBadRequest, "MissingParameter", "The request must contain the parameter Attributes.")
		return
	}

	err := h.engine.SetQueueAttributes(queueName, attrs)
	if err != nil {
		writeJSONQueueError(w, err)
		return
	}

	writeJSON(w, map[string]any{})
}

func queueNameFromURL(queueURL string) string {
	// Extract queue name from URL like http://host:port/accountId/queueName
	parts := strings.Split(strings.TrimRight(queueURL, "/"), "/")
	if len(parts) >= 1 {
		return parts[len(parts)-1]
	}
	return ""
}

func (h *Handler) sendMessageJSON(w http.ResponseWriter, raw map[string]json.RawMessage, pathQueueName string) {
	body := jsonString(raw, "MessageBody")
	if body == "" {
		writeJSONError(w, http.StatusBadRequest, "MissingParameter", "The request must contain the parameter MessageBody.")
		return
	}

	queueName := pathQueueName
	if queueName == "" {
		queueName = queueNameFromURL(jsonString(raw, "QueueUrl"))
	}
	if queueName == "" {
		writeJSONError(w, http.StatusBadRequest, "InvalidParameterValue", "Queue name is required.")
		return
	}

	delaySeconds := -1 // use queue default
	if v, ok := jsonInt(raw, "DelaySeconds"); ok {
		if v < 0 || v > 900 {
			writeJSONError(w, http.StatusBadRequest, "InvalidParameterValue", "Value for parameter DelaySeconds is invalid. Reason: Must be between 0 and 900.")
			return
		}
		delaySeconds = v
	}

	msg, err := h.engine.SendMessage(queueName, body, delaySeconds)
	if err != nil {
		writeJSONQueueError(w, err)
		return
	}

	writeJSON(w, map[string]string{
		"MessageId":        msg.MessageID,
		"MD5OfMessageBody": msg.MD5OfBody,
	})
}

func (h *Handler) receiveMessageJSON(w http.ResponseWriter, r *http.Request, raw map[string]json.RawMessage, pathQueueName string) {
	queueName := pathQueueName
	if queueName == "" {
		queueName = queueNameFromURL(jsonString(raw, "QueueUrl"))
	}
	if queueName == "" {
		writeJSONError(w, http.StatusBadRequest, "InvalidParameterValue", "Queue name is required.")
		return
	}

	maxMessages := 1
	if n, ok := jsonInt(raw, "MaxNumberOfMessages"); ok {
		if n < 1 || n > 10 {
			writeJSONError(w, http.StatusBadRequest, "InvalidParameterValue", "Value for parameter MaxNumberOfMessages is invalid. Reason: Must be between 1 and 10.")
			return
		}
		maxMessages = n
	}

	visibilityTimeout := -1
	if v, ok := jsonInt(raw, "VisibilityTimeout"); ok {
		if v < 0 {
			writeJSONError(w, http.StatusBadRequest, "InvalidParameterValue", "Value for parameter VisibilityTimeout is invalid.")
			return
		}
		visibilityTimeout = v
	}

	if visibilityTimeout < 0 {
		vt, err := h.engine.GetQueueVisibilityTimeout(queueName)
		if err != nil {
			writeJSONQueueError(w, err)
			return
		}
		visibilityTimeout = vt
	}

	waitTimeSeconds := -1
	if v, ok := jsonInt(raw, "WaitTimeSeconds"); ok {
		if v < 0 || v > 20 {
			writeJSONError(w, http.StatusBadRequest, "InvalidParameterValue", "Value for parameter WaitTimeSeconds is invalid. Reason: Must be between 0 and 20.")
			return
		}
		waitTimeSeconds = v
	}

	if waitTimeSeconds < 0 {
		wt, err := h.engine.GetQueueWaitTimeSeconds(queueName)
		if err != nil {
			writeJSONQueueError(w, err)
			return
		}
		waitTimeSeconds = wt
	}

	msgs, err := h.engine.ReceiveMessage(r.Context(), queueName, maxMessages, visibilityTimeout, waitTimeSeconds)
	if err != nil {
		writeJSONQueueError(w, err)
		return
	}

	type jsonMessage struct {
		MessageId     string            `json:"MessageId"`
		ReceiptHandle string            `json:"ReceiptHandle"`
		MD5OfBody     string            `json:"MD5OfBody"`
		Body          string            `json:"Body"`
		Attributes    map[string]string `json:"Attributes,omitempty"`
	}

	var messages []jsonMessage
	for _, msg := range msgs {
		messages = append(messages, jsonMessage{
			MessageId:     msg.MessageID,
			ReceiptHandle: msg.ReceiptHandle,
			MD5OfBody:     msg.MD5OfBody,
			Body:          msg.Body,
			Attributes: map[string]string{
				"SentTimestamp":                    strconv.FormatInt(msg.SentTimestamp, 10),
				"ApproximateReceiveCount":          strconv.Itoa(msg.ReceiveCount),
				"ApproximateFirstReceiveTimestamp": strconv.FormatInt(msg.FirstReceivedAt, 10),
			},
		})
	}

	writeJSON(w, map[string]any{"Messages": messages})
}

func (h *Handler) deleteMessageJSON(w http.ResponseWriter, raw map[string]json.RawMessage, pathQueueName string) {
	handle := jsonString(raw, "ReceiptHandle")
	if handle == "" {
		writeJSONError(w, http.StatusBadRequest, "MissingParameter", "The request must contain the parameter ReceiptHandle.")
		return
	}

	queueName := pathQueueName
	if queueName == "" {
		queueName = queueNameFromURL(jsonString(raw, "QueueUrl"))
	}
	if queueName == "" {
		writeJSONError(w, http.StatusBadRequest, "InvalidParameterValue", "Queue name is required.")
		return
	}

	err := h.engine.DeleteMessage(queueName, handle)
	if err != nil {
		writeJSONQueueError(w, err)
		return
	}

	writeJSON(w, map[string]any{})
}

// --- Query protocol (form-encoded + XML, used by PHP/older SDKs) ---

func (h *Handler) handleQuery(w http.ResponseWriter, r *http.Request, pathQueueName string) {
	params, err := query.ParseRequest(r)
	if err != nil {
		query.WriteError(w, http.StatusBadRequest, "InvalidParameterValue", "Failed to parse request body.")
		return
	}

	action := params.Action()

	switch action {
	case "CreateQueue":
		h.createQueueXML(w, params)
	case "GetQueueUrl":
		h.getQueueURLXML(w, params)
	case "GetQueueAttributes":
		h.getQueueAttributesXML(w, params, pathQueueName)
	case "SetQueueAttributes":
		h.setQueueAttributesXML(w, params, pathQueueName)
	case "SendMessage":
		h.sendMessageXML(w, params, pathQueueName)
	case "ReceiveMessage":
		h.receiveMessageXML(w, r, params, pathQueueName)
	case "DeleteMessage":
		h.deleteMessageXML(w, params, pathQueueName)
	case "SendMessageBatch":
		h.sendMessageBatchXML(w, params, pathQueueName)
	case "DeleteMessageBatch":
		h.deleteMessageBatchXML(w, params, pathQueueName)
	case "PurgeQueue":
		h.purgeQueueXML(w, params, pathQueueName)
	default:
		query.WriteError(w, http.StatusBadRequest, "InvalidAction", "The action "+action+" is not valid for this endpoint.")
	}
}

// --- XML types ---

type createQueueXMLResponse struct {
	XMLName  xml.Name             `xml:"CreateQueueResponse"`
	Result   createQueueXMLResult `xml:"CreateQueueResult"`
	Metadata query.ResponseMetadata
}

type createQueueXMLResult struct {
	QueueURL string `xml:"QueueUrl"`
}

func (h *Handler) createQueueXML(w http.ResponseWriter, params query.Params) {
	name := params.Get("QueueName")
	if name == "" {
		query.WriteError(w, http.StatusBadRequest, "InvalidParameterValue", "Value for parameter QueueName is invalid. Reason: Must specify a queue name.")
		return
	}

	q, err := h.engine.CreateQueue(name)
	if err != nil {
		query.WriteError(w, http.StatusInternalServerError, "InternalError", "An internal error occurred.")
		return
	}

	query.WriteXML(w, http.StatusOK, createQueueXMLResponse{
		Result:   createQueueXMLResult{QueueURL: q.URL},
		Metadata: query.ResponseMetadata{RequestID: query.NewRequestID()},
	})
}

type getQueueURLXMLResponse struct {
	XMLName  xml.Name             `xml:"GetQueueUrlResponse"`
	Result   getQueueURLXMLResult `xml:"GetQueueUrlResult"`
	Metadata query.ResponseMetadata
}

type getQueueURLXMLResult struct {
	QueueURL string `xml:"QueueUrl"`
}

func (h *Handler) getQueueURLXML(w http.ResponseWriter, params query.Params) {
	name := params.Get("QueueName")
	if name == "" {
		query.WriteError(w, http.StatusBadRequest, "InvalidParameterValue", "Value for parameter QueueName is invalid. Reason: Must specify a queue name.")
		return
	}

	url, err := h.engine.GetQueueURL(name)
	if err != nil {
		writeQueueErrorXML(w, err)
		return
	}

	query.WriteXML(w, http.StatusOK, getQueueURLXMLResponse{
		Result:   getQueueURLXMLResult{QueueURL: url},
		Metadata: query.ResponseMetadata{RequestID: query.NewRequestID()},
	})
}

type getQueueAttributesXMLResponse struct {
	XMLName  xml.Name                    `xml:"GetQueueAttributesResponse"`
	Result   getQueueAttributesXMLResult `xml:"GetQueueAttributesResult"`
	Metadata query.ResponseMetadata
}

type getQueueAttributesXMLResult struct {
	Attributes []xmlQueueAttribute `xml:"Attribute,omitempty"`
}

type xmlQueueAttribute struct {
	Name  string `xml:"Name"`
	Value string `xml:"Value"`
}

func (h *Handler) getQueueAttributesXML(w http.ResponseWriter, params query.Params, queueName string) {
	if queueName == "" {
		query.WriteError(w, http.StatusBadRequest, "InvalidParameterValue", "Queue name is required.")
		return
	}

	var attrNames []string
	for i := 1; ; i++ {
		name := params.Get(fmt.Sprintf("AttributeName.%d", i))
		if name == "" {
			break
		}
		attrNames = append(attrNames, name)
	}
	if len(attrNames) == 0 {
		attrNames = []string{"All"}
	}

	attrs, err := h.engine.GetQueueAttributes(queueName, attrNames)
	if err != nil {
		writeQueueErrorXML(w, err)
		return
	}

	var xmlAttrs []xmlQueueAttribute
	for k, v := range attrs {
		xmlAttrs = append(xmlAttrs, xmlQueueAttribute{Name: k, Value: v})
	}
	sort.Slice(xmlAttrs, func(i, j int) bool { return xmlAttrs[i].Name < xmlAttrs[j].Name })

	query.WriteXML(w, http.StatusOK, getQueueAttributesXMLResponse{
		Result:   getQueueAttributesXMLResult{Attributes: xmlAttrs},
		Metadata: query.ResponseMetadata{RequestID: query.NewRequestID()},
	})
}

type setQueueAttributesXMLResponse struct {
	XMLName  xml.Name `xml:"SetQueueAttributesResponse"`
	Metadata query.ResponseMetadata
}

func (h *Handler) setQueueAttributesXML(w http.ResponseWriter, params query.Params, queueName string) {
	if queueName == "" {
		query.WriteError(w, http.StatusBadRequest, "InvalidParameterValue", "Queue name is required.")
		return
	}

	attrs := make(map[string]string)
	for i := 1; ; i++ {
		name := params.Get(fmt.Sprintf("Attribute.%d.Name", i))
		if name == "" {
			break
		}
		value := params.Get(fmt.Sprintf("Attribute.%d.Value", i))
		attrs[name] = value
	}
	if len(attrs) == 0 {
		query.WriteError(w, http.StatusBadRequest, "MissingParameter", "The request must contain the parameter Attributes.")
		return
	}

	err := h.engine.SetQueueAttributes(queueName, attrs)
	if err != nil {
		writeQueueErrorXML(w, err)
		return
	}

	query.WriteXML(w, http.StatusOK, setQueueAttributesXMLResponse{
		Metadata: query.ResponseMetadata{RequestID: query.NewRequestID()},
	})
}

type sendMessageXMLResponse struct {
	XMLName  xml.Name             `xml:"SendMessageResponse"`
	Result   sendMessageXMLResult `xml:"SendMessageResult"`
	Metadata query.ResponseMetadata
}

type sendMessageXMLResult struct {
	MessageID string `xml:"MessageId"`
	MD5OfBody string `xml:"MD5OfMessageBody"`
}

func (h *Handler) sendMessageXML(w http.ResponseWriter, params query.Params, queueName string) {
	body := params.Get("MessageBody")
	if body == "" {
		query.WriteError(w, http.StatusBadRequest, "MissingParameter", "The request must contain the parameter MessageBody.")
		return
	}

	if queueName == "" {
		query.WriteError(w, http.StatusBadRequest, "InvalidParameterValue", "Queue name is required.")
		return
	}

	delaySeconds := -1 // use queue default
	if dsStr := params.Get("DelaySeconds"); dsStr != "" {
		v, err := strconv.Atoi(dsStr)
		if err != nil || v < 0 || v > 900 {
			query.WriteError(w, http.StatusBadRequest, "InvalidParameterValue", "Value for parameter DelaySeconds is invalid. Reason: Must be between 0 and 900.")
			return
		}
		delaySeconds = v
	}

	msg, err := h.engine.SendMessage(queueName, body, delaySeconds)
	if err != nil {
		writeQueueErrorXML(w, err)
		return
	}

	query.WriteXML(w, http.StatusOK, sendMessageXMLResponse{
		Result: sendMessageXMLResult{
			MessageID: msg.MessageID,
			MD5OfBody: msg.MD5OfBody,
		},
		Metadata: query.ResponseMetadata{RequestID: query.NewRequestID()},
	})
}

type receiveMessageXMLResponse struct {
	XMLName  xml.Name                `xml:"ReceiveMessageResponse"`
	Result   receiveMessageXMLResult `xml:"ReceiveMessageResult"`
	Metadata query.ResponseMetadata
}

type receiveMessageXMLResult struct {
	Messages []receiveMessageXMLEntry `xml:"Message,omitempty"`
}

type receiveMessageXMLEntry struct {
	MessageID     string                `xml:"MessageId"`
	ReceiptHandle string                `xml:"ReceiptHandle"`
	MD5OfBody     string                `xml:"MD5OfBody"`
	Body          string                `xml:"Body"`
	Attributes    []xmlMessageAttribute `xml:"Attribute,omitempty"`
}

type xmlMessageAttribute struct {
	Name  string `xml:"Name"`
	Value string `xml:"Value"`
}

func (h *Handler) receiveMessageXML(w http.ResponseWriter, r *http.Request, params query.Params, queueName string) {
	if queueName == "" {
		query.WriteError(w, http.StatusBadRequest, "InvalidParameterValue", "Queue name is required.")
		return
	}

	maxStr := params.Get("MaxNumberOfMessages")
	maxMessages := 1
	if maxStr != "" {
		n, err := strconv.Atoi(maxStr)
		if err != nil || n < 1 || n > 10 {
			query.WriteError(w, http.StatusBadRequest, "InvalidParameterValue", "Value for parameter MaxNumberOfMessages is invalid. Reason: Must be between 1 and 10.")
			return
		}
		maxMessages = n
	}

	visStr := params.Get("VisibilityTimeout")
	visibilityTimeout := -1
	if visStr != "" {
		v, err := strconv.Atoi(visStr)
		if err != nil || v < 0 {
			query.WriteError(w, http.StatusBadRequest, "InvalidParameterValue", "Value for parameter VisibilityTimeout is invalid.")
			return
		}
		visibilityTimeout = v
	}

	if visibilityTimeout < 0 {
		vt, err := h.engine.GetQueueVisibilityTimeout(queueName)
		if err != nil {
			writeQueueErrorXML(w, err)
			return
		}
		visibilityTimeout = vt
	}

	waitStr := params.Get("WaitTimeSeconds")
	waitTimeSeconds := -1
	if waitStr != "" {
		v, err := strconv.Atoi(waitStr)
		if err != nil || v < 0 || v > 20 {
			query.WriteError(w, http.StatusBadRequest, "InvalidParameterValue", "Value for parameter WaitTimeSeconds is invalid. Reason: Must be between 0 and 20.")
			return
		}
		waitTimeSeconds = v
	}

	if waitTimeSeconds < 0 {
		wt, err := h.engine.GetQueueWaitTimeSeconds(queueName)
		if err != nil {
			writeQueueErrorXML(w, err)
			return
		}
		waitTimeSeconds = wt
	}

	msgs, err := h.engine.ReceiveMessage(r.Context(), queueName, maxMessages, visibilityTimeout, waitTimeSeconds)
	if err != nil {
		writeQueueErrorXML(w, err)
		return
	}

	var entries []receiveMessageXMLEntry
	for _, msg := range msgs {
		entries = append(entries, receiveMessageXMLEntry{
			MessageID:     msg.MessageID,
			ReceiptHandle: msg.ReceiptHandle,
			MD5OfBody:     msg.MD5OfBody,
			Body:          msg.Body,
			Attributes: []xmlMessageAttribute{
				{Name: "SentTimestamp", Value: strconv.FormatInt(msg.SentTimestamp, 10)},
				{Name: "ApproximateReceiveCount", Value: strconv.Itoa(msg.ReceiveCount)},
				{Name: "ApproximateFirstReceiveTimestamp", Value: strconv.FormatInt(msg.FirstReceivedAt, 10)},
			},
		})
	}

	query.WriteXML(w, http.StatusOK, receiveMessageXMLResponse{
		Result:   receiveMessageXMLResult{Messages: entries},
		Metadata: query.ResponseMetadata{RequestID: query.NewRequestID()},
	})
}

type deleteMessageXMLResponse struct {
	XMLName  xml.Name `xml:"DeleteMessageResponse"`
	Metadata query.ResponseMetadata
}

func (h *Handler) deleteMessageXML(w http.ResponseWriter, params query.Params, queueName string) {
	handle := params.Get("ReceiptHandle")
	if handle == "" {
		query.WriteError(w, http.StatusBadRequest, "MissingParameter", "The request must contain the parameter ReceiptHandle.")
		return
	}

	if queueName == "" {
		query.WriteError(w, http.StatusBadRequest, "InvalidParameterValue", "Queue name is required.")
		return
	}

	err := h.engine.DeleteMessage(queueName, handle)
	if err != nil {
		writeQueueErrorXML(w, err)
		return
	}

	query.WriteXML(w, http.StatusOK, deleteMessageXMLResponse{
		Metadata: query.ResponseMetadata{RequestID: query.NewRequestID()},
	})
}

func writeQueueErrorXML(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, ErrQueueDoesNotExist):
		query.WriteError(w, http.StatusBadRequest, "AWS.SimpleQueueService.NonExistentQueue", "The specified queue does not exist.")
	case errors.Is(err, ErrReceiptHandleIsInvalid):
		query.WriteError(w, http.StatusBadRequest, "ReceiptHandleIsInvalid", "The input receipt handle is invalid.")
	case errors.Is(err, ErrEmptyBatchRequest):
		query.WriteError(w, http.StatusBadRequest, "AWS.SimpleQueueService.EmptyBatchRequest", sanitizeErrorMessage(err))
	case errors.Is(err, ErrTooManyEntriesInBatchRequest):
		query.WriteError(w, http.StatusBadRequest, "AWS.SimpleQueueService.TooManyEntriesInBatchRequest", sanitizeErrorMessage(err))
	case errors.Is(err, ErrBatchEntryIdsNotDistinct):
		query.WriteError(w, http.StatusBadRequest, "AWS.SimpleQueueService.BatchEntryIdsNotDistinct", sanitizeErrorMessage(err))
	case errors.Is(err, ErrPurgeQueueInProgress):
		query.WriteError(w, http.StatusForbidden, "AWS.SimpleQueueService.PurgeQueueInProgress", "Only one PurgeQueue operation is allowed every 60 seconds.")
	case errors.Is(err, ErrInvalidParameterValue):
		query.WriteError(w, http.StatusBadRequest, "InvalidParameterValue", sanitizeErrorMessage(err))
	default:
		query.WriteError(w, http.StatusInternalServerError, "InternalError", "An internal error occurred.")
	}
}

// --- SendMessageBatch handlers ---

func (h *Handler) sendMessageBatchJSON(w http.ResponseWriter, raw map[string]json.RawMessage, pathQueueName string) {
	queueName := pathQueueName
	if queueName == "" {
		queueName = queueNameFromURL(jsonString(raw, "QueueUrl"))
	}
	if queueName == "" {
		writeJSONError(w, http.StatusBadRequest, "InvalidParameterValue", "Queue name is required.")
		return
	}

	type jsonBatchEntry struct {
		Id           string `json:"Id"`
		MessageBody  string `json:"MessageBody"`
		DelaySeconds *int   `json:"DelaySeconds,omitempty"`
	}

	var entries []jsonBatchEntry
	if v, ok := raw["Entries"]; ok {
		if err := json.Unmarshal(v, &entries); err != nil {
			writeJSONError(w, http.StatusBadRequest, "InvalidParameterValue", "Failed to parse Entries.")
			return
		}
	}

	batchEntries := make([]SendMessageBatchEntry, len(entries))
	for i, e := range entries {
		ds := -1
		if e.DelaySeconds != nil {
			ds = *e.DelaySeconds
		}
		batchEntries[i] = SendMessageBatchEntry{
			ID:           e.Id,
			Body:         e.MessageBody,
			DelaySeconds: ds,
		}
	}

	result, err := h.engine.SendMessageBatch(queueName, batchEntries)
	if err != nil {
		writeJSONQueueError(w, err)
		return
	}

	type successEntry struct {
		Id               string `json:"Id"`
		MessageId        string `json:"MessageId"`
		MD5OfMessageBody string `json:"MD5OfMessageBody"`
	}
	type failEntry struct {
		Id          string `json:"Id"`
		SenderFault bool   `json:"SenderFault"`
		Code        string `json:"Code"`
		Message     string `json:"Message"`
	}

	successful := make([]successEntry, 0, len(result.Successful))
	for _, s := range result.Successful {
		successful = append(successful, successEntry{
			Id:               s.ID,
			MessageId:        s.MessageID,
			MD5OfMessageBody: s.MD5OfBody,
		})
	}
	failed := make([]failEntry, 0, len(result.Failed))
	for _, f := range result.Failed {
		failed = append(failed, failEntry{
			Id:          f.ID,
			SenderFault: f.SenderFault,
			Code:        f.Code,
			Message:     f.Message,
		})
	}

	writeJSON(w, map[string]any{
		"Successful": successful,
		"Failed":     failed,
	})
}

type sendMessageBatchXMLResponse struct {
	XMLName  xml.Name                  `xml:"SendMessageBatchResponse"`
	Result   sendMessageBatchXMLResult `xml:"SendMessageBatchResult"`
	Metadata query.ResponseMetadata
}

type sendMessageBatchXMLResult struct {
	Successful []sendMessageBatchXMLSuccess `xml:"SendMessageBatchResultEntry,omitempty"`
	Failed     []batchXMLError              `xml:"BatchResultErrorEntry,omitempty"`
}

type sendMessageBatchXMLSuccess struct {
	ID        string `xml:"Id"`
	MessageID string `xml:"MessageId"`
	MD5OfBody string `xml:"MD5OfMessageBody"`
}

type batchXMLError struct {
	ID          string `xml:"Id"`
	SenderFault bool   `xml:"SenderFault"`
	Code        string `xml:"Code"`
	Message     string `xml:"Message"`
}

func (h *Handler) sendMessageBatchXML(w http.ResponseWriter, params query.Params, queueName string) {
	if queueName == "" {
		query.WriteError(w, http.StatusBadRequest, "InvalidParameterValue", "Queue name is required.")
		return
	}

	var entries []SendMessageBatchEntry
	for i := 1; ; i++ {
		id := params.Get(fmt.Sprintf("SendMessageBatchRequestEntry.%d.Id", i))
		if id == "" {
			break
		}
		body := params.Get(fmt.Sprintf("SendMessageBatchRequestEntry.%d.MessageBody", i))
		ds := -1
		if dsStr := params.Get(fmt.Sprintf("SendMessageBatchRequestEntry.%d.DelaySeconds", i)); dsStr != "" {
			v, err := strconv.Atoi(dsStr)
			if err != nil {
				query.WriteError(w, http.StatusBadRequest, "InvalidParameterValue", "Value for parameter DelaySeconds is invalid. Reason: Must be between 0 and 900.")
				return
			}
			ds = v
		}
		entries = append(entries, SendMessageBatchEntry{
			ID:           id,
			Body:         body,
			DelaySeconds: ds,
		})
	}

	result, err := h.engine.SendMessageBatch(queueName, entries)
	if err != nil {
		writeQueueErrorXML(w, err)
		return
	}

	var successful []sendMessageBatchXMLSuccess
	for _, s := range result.Successful {
		successful = append(successful, sendMessageBatchXMLSuccess(s))
	}
	var failed []batchXMLError
	for _, f := range result.Failed {
		failed = append(failed, batchXMLError(f))
	}

	query.WriteXML(w, http.StatusOK, sendMessageBatchXMLResponse{
		Result: sendMessageBatchXMLResult{
			Successful: successful,
			Failed:     failed,
		},
		Metadata: query.ResponseMetadata{RequestID: query.NewRequestID()},
	})
}

// --- DeleteMessageBatch handlers ---

func (h *Handler) deleteMessageBatchJSON(w http.ResponseWriter, raw map[string]json.RawMessage, pathQueueName string) {
	queueName := pathQueueName
	if queueName == "" {
		queueName = queueNameFromURL(jsonString(raw, "QueueUrl"))
	}
	if queueName == "" {
		writeJSONError(w, http.StatusBadRequest, "InvalidParameterValue", "Queue name is required.")
		return
	}

	type jsonBatchEntry struct {
		Id            string `json:"Id"`
		ReceiptHandle string `json:"ReceiptHandle"`
	}

	var entries []jsonBatchEntry
	if v, ok := raw["Entries"]; ok {
		if err := json.Unmarshal(v, &entries); err != nil {
			writeJSONError(w, http.StatusBadRequest, "InvalidParameterValue", "Failed to parse Entries.")
			return
		}
	}

	batchEntries := make([]DeleteMessageBatchEntry, len(entries))
	for i, e := range entries {
		batchEntries[i] = DeleteMessageBatchEntry{
			ID:            e.Id,
			ReceiptHandle: e.ReceiptHandle,
		}
	}

	result, err := h.engine.DeleteMessageBatch(queueName, batchEntries)
	if err != nil {
		writeJSONQueueError(w, err)
		return
	}

	type successEntry struct {
		Id string `json:"Id"`
	}
	type failEntry struct {
		Id          string `json:"Id"`
		SenderFault bool   `json:"SenderFault"`
		Code        string `json:"Code"`
		Message     string `json:"Message"`
	}

	successful := make([]successEntry, 0, len(result.Successful))
	for _, s := range result.Successful {
		successful = append(successful, successEntry{Id: s.ID})
	}
	failed := make([]failEntry, 0, len(result.Failed))
	for _, f := range result.Failed {
		failed = append(failed, failEntry{
			Id:          f.ID,
			SenderFault: f.SenderFault,
			Code:        f.Code,
			Message:     f.Message,
		})
	}

	writeJSON(w, map[string]any{
		"Successful": successful,
		"Failed":     failed,
	})
}

type deleteMessageBatchXMLResponse struct {
	XMLName  xml.Name                    `xml:"DeleteMessageBatchResponse"`
	Result   deleteMessageBatchXMLResult `xml:"DeleteMessageBatchResult"`
	Metadata query.ResponseMetadata
}

type deleteMessageBatchXMLResult struct {
	Successful []deleteMessageBatchXMLSuccess `xml:"DeleteMessageBatchResultEntry,omitempty"`
	Failed     []batchXMLError                `xml:"BatchResultErrorEntry,omitempty"`
}

type deleteMessageBatchXMLSuccess struct {
	ID string `xml:"Id"`
}

func (h *Handler) deleteMessageBatchXML(w http.ResponseWriter, params query.Params, queueName string) {
	if queueName == "" {
		query.WriteError(w, http.StatusBadRequest, "InvalidParameterValue", "Queue name is required.")
		return
	}

	var entries []DeleteMessageBatchEntry
	for i := 1; ; i++ {
		id := params.Get(fmt.Sprintf("DeleteMessageBatchRequestEntry.%d.Id", i))
		if id == "" {
			break
		}
		handle := params.Get(fmt.Sprintf("DeleteMessageBatchRequestEntry.%d.ReceiptHandle", i))
		entries = append(entries, DeleteMessageBatchEntry{
			ID:            id,
			ReceiptHandle: handle,
		})
	}

	result, err := h.engine.DeleteMessageBatch(queueName, entries)
	if err != nil {
		writeQueueErrorXML(w, err)
		return
	}

	var successful []deleteMessageBatchXMLSuccess
	for _, s := range result.Successful {
		successful = append(successful, deleteMessageBatchXMLSuccess{ID: s.ID})
	}
	var failed []batchXMLError
	for _, f := range result.Failed {
		failed = append(failed, batchXMLError(f))
	}

	query.WriteXML(w, http.StatusOK, deleteMessageBatchXMLResponse{
		Result: deleteMessageBatchXMLResult{
			Successful: successful,
			Failed:     failed,
		},
		Metadata: query.ResponseMetadata{RequestID: query.NewRequestID()},
	})
}

// --- PurgeQueue handlers ---

func (h *Handler) purgeQueueJSON(w http.ResponseWriter, raw map[string]json.RawMessage, pathQueueName string) {
	queueName := pathQueueName
	if queueName == "" {
		queueName = queueNameFromURL(jsonString(raw, "QueueUrl"))
	}
	if queueName == "" {
		writeJSONError(w, http.StatusBadRequest, "InvalidParameterValue", "Queue name is required.")
		return
	}

	if err := h.engine.PurgeQueue(queueName); err != nil {
		writeJSONQueueError(w, err)
		return
	}
	writeJSON(w, map[string]any{})
}

type purgeQueueXMLResponse struct {
	XMLName  xml.Name `xml:"PurgeQueueResponse"`
	Metadata query.ResponseMetadata
}

func (h *Handler) purgeQueueXML(w http.ResponseWriter, params query.Params, queueName string) {
	if queueName == "" {
		queueName = queueNameFromURL(params.Get("QueueUrl"))
	}
	if queueName == "" {
		query.WriteError(w, http.StatusBadRequest, "InvalidParameterValue", "Queue name is required.")
		return
	}

	if err := h.engine.PurgeQueue(queueName); err != nil {
		writeQueueErrorXML(w, err)
		return
	}

	query.WriteXML(w, http.StatusOK, purgeQueueXMLResponse{
		Metadata: query.ResponseMetadata{RequestID: query.NewRequestID()},
	})
}
