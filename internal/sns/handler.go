package sns

import (
	"encoding/json"
	"encoding/xml"
	"errors"
	"fmt"
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
	case "SetSubscriptionAttributes":
		h.setSubscriptionAttributesJSON(w, raw)
	case "GetSubscriptionAttributes":
		h.getSubscriptionAttributesJSON(w, raw)
	case "ListTopics":
		h.listTopicsJSON(w)
	case "DeleteTopic":
		h.deleteTopicJSON(w, raw)
	case "ListSubscriptionsByTopic":
		h.listSubscriptionsByTopicJSON(w, raw)
	case "Unsubscribe":
		h.unsubscribeJSON(w, raw)
	case "GetTopicAttributes":
		h.getTopicAttributesJSON(w, raw)
	case "SetTopicAttributes":
		h.setTopicAttributesJSON(w, raw)
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
	case errors.Is(err, ErrSubscriptionNotFound):
		writeJSONError(w, http.StatusNotFound, "NotFound", "Subscription does not exist.")
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

	msgAttrs, err := parseMessageAttributesJSON(raw)
	if err != nil {
		writeSNSJSONError(w, err)
		return
	}

	result, err := h.engine.Publish(topicARN, message, subject, msgAttrs)
	if err != nil {
		writeSNSJSONError(w, err)
		return
	}

	writeJSON(w, map[string]string{"MessageId": result.MessageID})
}

func parseMessageAttributesJSON(raw map[string]json.RawMessage) (map[string]MessageAttribute, error) {
	v, ok := raw["MessageAttributes"]
	if !ok {
		return map[string]MessageAttribute{}, nil
	}
	var rawAttrs map[string]struct {
		DataType    string `json:"DataType"`
		StringValue string `json:"StringValue"`
	}
	if err := json.Unmarshal(v, &rawAttrs); err != nil {
		return nil, fmt.Errorf("%w: Invalid MessageAttributes: %s", ErrInvalidParameter, err.Error())
	}
	attrs := make(map[string]MessageAttribute, len(rawAttrs))
	for k, a := range rawAttrs {
		attrs[k] = MessageAttribute{
			DataType:    a.DataType,
			StringValue: a.StringValue,
		}
	}
	return attrs, nil
}

func (h *Handler) setSubscriptionAttributesJSON(w http.ResponseWriter, raw map[string]json.RawMessage) {
	subARN := jsonString(raw, "SubscriptionArn")
	attrName := jsonString(raw, "AttributeName")
	attrValue := jsonString(raw, "AttributeValue")

	if subARN == "" {
		writeJSONError(w, http.StatusBadRequest, "InvalidParameter", "SubscriptionArn is required.")
		return
	}
	if attrName == "" {
		writeJSONError(w, http.StatusBadRequest, "InvalidParameter", "AttributeName is required.")
		return
	}

	err := h.engine.SetSubscriptionAttributes(subARN, attrName, attrValue)
	if err != nil {
		writeSNSJSONError(w, err)
		return
	}

	writeJSON(w, map[string]any{})
}

func (h *Handler) getSubscriptionAttributesJSON(w http.ResponseWriter, raw map[string]json.RawMessage) {
	subARN := jsonString(raw, "SubscriptionArn")

	if subARN == "" {
		writeJSONError(w, http.StatusBadRequest, "InvalidParameter", "SubscriptionArn is required.")
		return
	}

	attrs, err := h.engine.GetSubscriptionAttributes(subARN)
	if err != nil {
		writeSNSJSONError(w, err)
		return
	}

	writeJSON(w, map[string]any{"Attributes": attrs})
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
	case "SetSubscriptionAttributes":
		h.setSubscriptionAttributesXML(w, params)
	case "GetSubscriptionAttributes":
		h.getSubscriptionAttributesXML(w, params)
	case "ListTopics":
		h.listTopicsXML(w)
	case "DeleteTopic":
		h.deleteTopicXML(w, params)
	case "ListSubscriptionsByTopic":
		h.listSubscriptionsByTopicXML(w, params)
	case "Unsubscribe":
		h.unsubscribeXML(w, params)
	case "GetTopicAttributes":
		h.getTopicAttributesXML(w, params)
	case "SetTopicAttributes":
		h.setTopicAttributesXML(w, params)
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

	msgAttrs, err := parseMessageAttributesQuery(params)
	if err != nil {
		writeSNSErrorXML(w, err)
		return
	}

	result, err := h.engine.Publish(topicARN, message, subject, msgAttrs)
	if err != nil {
		writeSNSErrorXML(w, err)
		return
	}

	query.WriteXML(w, http.StatusOK, publishXMLResponse{
		Result:   publishXMLResult{MessageID: result.MessageID},
		Metadata: query.ResponseMetadata{RequestID: query.NewRequestID()},
	})
}

func parseMessageAttributesQuery(params query.Params) (map[string]MessageAttribute, error) {
	var attrs map[string]MessageAttribute
	for i := 1; ; i++ {
		name := params.Get(fmt.Sprintf("MessageAttributes.entry.%d.Name", i))
		if name == "" {
			break
		}
		dataType := params.Get(fmt.Sprintf("MessageAttributes.entry.%d.Value.DataType", i))
		if dataType == "" {
			return nil, fmt.Errorf("%w: DataType is required for MessageAttribute '%s'", ErrInvalidParameter, name)
		}
		if attrs == nil {
			attrs = make(map[string]MessageAttribute)
		}
		stringValue := params.Get(fmt.Sprintf("MessageAttributes.entry.%d.Value.StringValue", i))
		attrs[name] = MessageAttribute{
			DataType:    dataType,
			StringValue: stringValue,
		}
	}
	return attrs, nil
}

type setSubscriptionAttributesXMLResponse struct {
	XMLName  xml.Name `xml:"SetSubscriptionAttributesResponse"`
	Metadata query.ResponseMetadata
}

func (h *Handler) setSubscriptionAttributesXML(w http.ResponseWriter, params query.Params) {
	subARN := params.Get("SubscriptionArn")
	attrName := params.Get("AttributeName")
	attrValue := params.Get("AttributeValue")

	if subARN == "" {
		query.WriteError(w, http.StatusBadRequest, "InvalidParameter", "SubscriptionArn is required.")
		return
	}
	if attrName == "" {
		query.WriteError(w, http.StatusBadRequest, "InvalidParameter", "AttributeName is required.")
		return
	}

	err := h.engine.SetSubscriptionAttributes(subARN, attrName, attrValue)
	if err != nil {
		writeSNSErrorXML(w, err)
		return
	}

	query.WriteXML(w, http.StatusOK, setSubscriptionAttributesXMLResponse{
		Metadata: query.ResponseMetadata{RequestID: query.NewRequestID()},
	})
}

type getSubscriptionAttributesXMLResponse struct {
	XMLName  xml.Name                           `xml:"GetSubscriptionAttributesResponse"`
	Result   getSubscriptionAttributesXMLResult `xml:"GetSubscriptionAttributesResult"`
	Metadata query.ResponseMetadata
}

type getSubscriptionAttributesXMLResult struct {
	Attributes []xmlSubscriptionAttribute `xml:"Attributes>entry,omitempty"`
}

type xmlSubscriptionAttribute struct {
	Key   string `xml:"key"`
	Value string `xml:"value"`
}

func (h *Handler) getSubscriptionAttributesXML(w http.ResponseWriter, params query.Params) {
	subARN := params.Get("SubscriptionArn")

	if subARN == "" {
		query.WriteError(w, http.StatusBadRequest, "InvalidParameter", "SubscriptionArn is required.")
		return
	}

	attrs, err := h.engine.GetSubscriptionAttributes(subARN)
	if err != nil {
		writeSNSErrorXML(w, err)
		return
	}

	var xmlAttrs []xmlSubscriptionAttribute
	for k, v := range attrs {
		xmlAttrs = append(xmlAttrs, xmlSubscriptionAttribute{Key: k, Value: v})
	}

	query.WriteXML(w, http.StatusOK, getSubscriptionAttributesXMLResponse{
		Result:   getSubscriptionAttributesXMLResult{Attributes: xmlAttrs},
		Metadata: query.ResponseMetadata{RequestID: query.NewRequestID()},
	})
}

func writeSNSErrorXML(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, ErrSubscriptionNotFound):
		query.WriteError(w, http.StatusNotFound, "NotFound", "Subscription does not exist.")
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

// --- ListTopics handlers ---

func (h *Handler) listTopicsJSON(w http.ResponseWriter) {
	arns := h.engine.ListTopics()

	type topicEntry struct {
		TopicArn string `json:"TopicArn"`
	}
	topics := make([]topicEntry, len(arns))
	for i, arn := range arns {
		topics[i] = topicEntry{TopicArn: arn}
	}
	writeJSON(w, map[string]any{"Topics": topics})
}

type listTopicsXMLResponse struct {
	XMLName  xml.Name            `xml:"ListTopicsResponse"`
	Result   listTopicsXMLResult `xml:"ListTopicsResult"`
	Metadata query.ResponseMetadata
}

type listTopicsXMLResult struct {
	Topics []listTopicsXMLMember `xml:"Topics>member,omitempty"`
}

type listTopicsXMLMember struct {
	TopicARN string `xml:"TopicArn"`
}

func (h *Handler) listTopicsXML(w http.ResponseWriter) {
	arns := h.engine.ListTopics()

	members := make([]listTopicsXMLMember, len(arns))
	for i, arn := range arns {
		members[i] = listTopicsXMLMember{TopicARN: arn}
	}

	query.WriteXML(w, http.StatusOK, listTopicsXMLResponse{
		Result:   listTopicsXMLResult{Topics: members},
		Metadata: query.ResponseMetadata{RequestID: query.NewRequestID()},
	})
}

// --- DeleteTopic handlers ---

func (h *Handler) deleteTopicJSON(w http.ResponseWriter, raw map[string]json.RawMessage) {
	topicARN := jsonString(raw, "TopicArn")
	if topicARN == "" {
		writeJSONError(w, http.StatusBadRequest, "InvalidParameter", "TopicArn is required.")
		return
	}

	if err := h.engine.DeleteTopic(topicARN); err != nil {
		writeSNSJSONError(w, err)
		return
	}
	writeJSON(w, map[string]any{})
}

type deleteTopicXMLResponse struct {
	XMLName  xml.Name `xml:"DeleteTopicResponse"`
	Metadata query.ResponseMetadata
}

func (h *Handler) deleteTopicXML(w http.ResponseWriter, params query.Params) {
	topicARN := params.Get("TopicArn")
	if topicARN == "" {
		query.WriteError(w, http.StatusBadRequest, "InvalidParameter", "TopicArn is required.")
		return
	}

	if err := h.engine.DeleteTopic(topicARN); err != nil {
		writeSNSErrorXML(w, err)
		return
	}

	query.WriteXML(w, http.StatusOK, deleteTopicXMLResponse{
		Metadata: query.ResponseMetadata{RequestID: query.NewRequestID()},
	})
}

// --- ListSubscriptionsByTopic handlers ---

func (h *Handler) listSubscriptionsByTopicJSON(w http.ResponseWriter, raw map[string]json.RawMessage) {
	topicARN := jsonString(raw, "TopicArn")
	if topicARN == "" {
		writeJSONError(w, http.StatusBadRequest, "InvalidParameter", "TopicArn is required.")
		return
	}

	subs, err := h.engine.ListSubscriptionsByTopic(topicARN)
	if err != nil {
		writeSNSJSONError(w, err)
		return
	}

	writeJSON(w, map[string]any{"Subscriptions": subs})
}

type listSubscriptionsByTopicXMLResponse struct {
	XMLName  xml.Name                          `xml:"ListSubscriptionsByTopicResponse"`
	Result   listSubscriptionsByTopicXMLResult `xml:"ListSubscriptionsByTopicResult"`
	Metadata query.ResponseMetadata
}

type listSubscriptionsByTopicXMLResult struct {
	Subscriptions []listSubscriptionsXMLMember `xml:"Subscriptions>member,omitempty"`
}

type listSubscriptionsXMLMember struct {
	SubscriptionARN string `xml:"SubscriptionArn"`
	TopicARN        string `xml:"TopicArn"`
	Protocol        string `xml:"Protocol"`
	Endpoint        string `xml:"Endpoint"`
	Owner           string `xml:"Owner"`
}

func (h *Handler) listSubscriptionsByTopicXML(w http.ResponseWriter, params query.Params) {
	topicARN := params.Get("TopicArn")
	if topicARN == "" {
		query.WriteError(w, http.StatusBadRequest, "InvalidParameter", "TopicArn is required.")
		return
	}

	subs, err := h.engine.ListSubscriptionsByTopic(topicARN)
	if err != nil {
		writeSNSErrorXML(w, err)
		return
	}

	members := make([]listSubscriptionsXMLMember, len(subs))
	for i, sub := range subs {
		members[i] = listSubscriptionsXMLMember{
			SubscriptionARN: sub["SubscriptionArn"],
			TopicARN:        sub["TopicArn"],
			Protocol:        sub["Protocol"],
			Endpoint:        sub["Endpoint"],
			Owner:           sub["Owner"],
		}
	}

	query.WriteXML(w, http.StatusOK, listSubscriptionsByTopicXMLResponse{
		Result:   listSubscriptionsByTopicXMLResult{Subscriptions: members},
		Metadata: query.ResponseMetadata{RequestID: query.NewRequestID()},
	})
}

// --- Unsubscribe handlers ---

func (h *Handler) unsubscribeJSON(w http.ResponseWriter, raw map[string]json.RawMessage) {
	subARN := jsonString(raw, "SubscriptionArn")
	if subARN == "" {
		writeJSONError(w, http.StatusBadRequest, "InvalidParameter", "SubscriptionArn is required.")
		return
	}

	if err := h.engine.Unsubscribe(subARN); err != nil {
		writeSNSJSONError(w, err)
		return
	}
	writeJSON(w, map[string]any{})
}

type unsubscribeXMLResponse struct {
	XMLName  xml.Name `xml:"UnsubscribeResponse"`
	Metadata query.ResponseMetadata
}

func (h *Handler) unsubscribeXML(w http.ResponseWriter, params query.Params) {
	subARN := params.Get("SubscriptionArn")
	if subARN == "" {
		query.WriteError(w, http.StatusBadRequest, "InvalidParameter", "SubscriptionArn is required.")
		return
	}

	if err := h.engine.Unsubscribe(subARN); err != nil {
		writeSNSErrorXML(w, err)
		return
	}

	query.WriteXML(w, http.StatusOK, unsubscribeXMLResponse{
		Metadata: query.ResponseMetadata{RequestID: query.NewRequestID()},
	})
}

// --- GetTopicAttributes handlers ---

func (h *Handler) getTopicAttributesJSON(w http.ResponseWriter, raw map[string]json.RawMessage) {
	topicARN := jsonString(raw, "TopicArn")
	if topicARN == "" {
		writeJSONError(w, http.StatusBadRequest, "InvalidParameter", "TopicArn is required.")
		return
	}

	attrs, err := h.engine.GetTopicAttributes(topicARN)
	if err != nil {
		writeSNSJSONError(w, err)
		return
	}

	writeJSON(w, map[string]any{"Attributes": attrs})
}

type getTopicAttributesXMLResponse struct {
	XMLName  xml.Name                    `xml:"GetTopicAttributesResponse"`
	Result   getTopicAttributesXMLResult `xml:"GetTopicAttributesResult"`
	Metadata query.ResponseMetadata
}

type getTopicAttributesXMLResult struct {
	Attributes []xmlTopicAttribute `xml:"Attributes>entry,omitempty"`
}

type xmlTopicAttribute struct {
	Key   string `xml:"key"`
	Value string `xml:"value"`
}

func (h *Handler) getTopicAttributesXML(w http.ResponseWriter, params query.Params) {
	topicARN := params.Get("TopicArn")
	if topicARN == "" {
		query.WriteError(w, http.StatusBadRequest, "InvalidParameter", "TopicArn is required.")
		return
	}

	attrs, err := h.engine.GetTopicAttributes(topicARN)
	if err != nil {
		writeSNSErrorXML(w, err)
		return
	}

	var xmlAttrs []xmlTopicAttribute
	for k, v := range attrs {
		xmlAttrs = append(xmlAttrs, xmlTopicAttribute{Key: k, Value: v})
	}

	query.WriteXML(w, http.StatusOK, getTopicAttributesXMLResponse{
		Result:   getTopicAttributesXMLResult{Attributes: xmlAttrs},
		Metadata: query.ResponseMetadata{RequestID: query.NewRequestID()},
	})
}

// --- SetTopicAttributes handlers ---

func (h *Handler) setTopicAttributesJSON(w http.ResponseWriter, raw map[string]json.RawMessage) {
	topicARN := jsonString(raw, "TopicArn")
	attrName := jsonString(raw, "AttributeName")
	attrValue := jsonString(raw, "AttributeValue")

	if topicARN == "" {
		writeJSONError(w, http.StatusBadRequest, "InvalidParameter", "TopicArn is required.")
		return
	}
	if attrName == "" {
		writeJSONError(w, http.StatusBadRequest, "InvalidParameter", "AttributeName is required.")
		return
	}

	err := h.engine.SetTopicAttributes(topicARN, attrName, attrValue)
	if err != nil {
		writeSNSJSONError(w, err)
		return
	}

	writeJSON(w, map[string]any{})
}

type setTopicAttributesXMLResponse struct {
	XMLName  xml.Name `xml:"SetTopicAttributesResponse"`
	Metadata query.ResponseMetadata
}

func (h *Handler) setTopicAttributesXML(w http.ResponseWriter, params query.Params) {
	topicARN := params.Get("TopicArn")
	attrName := params.Get("AttributeName")
	attrValue := params.Get("AttributeValue")

	if topicARN == "" {
		query.WriteError(w, http.StatusBadRequest, "InvalidParameter", "TopicArn is required.")
		return
	}
	if attrName == "" {
		query.WriteError(w, http.StatusBadRequest, "InvalidParameter", "AttributeName is required.")
		return
	}

	err := h.engine.SetTopicAttributes(topicARN, attrName, attrValue)
	if err != nil {
		writeSNSErrorXML(w, err)
		return
	}

	query.WriteXML(w, http.StatusOK, setTopicAttributesXMLResponse{
		Metadata: query.ResponseMetadata{RequestID: query.NewRequestID()},
	})
}
