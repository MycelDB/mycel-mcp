package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	mycel "github.com/myceldb/mycel-go-sdk"
	clientv1 "github.com/myceldb/mycel-go-sdk/gen/go/mycel/client/v1"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"
)

type nodeWriteArgs struct {
	SpaceID        string         `json:"space_id,omitempty"`
	DomainID       string         `json:"domain_id,omitempty"`
	NodeID         string         `json:"node_id,omitempty"`
	Labels         []string       `json:"labels,omitempty"`
	Properties     map[string]any `json:"properties,omitempty"`
	Payload        map[string]any `json:"payload,omitempty"`
	Meta           map[string]any `json:"meta,omitempty"`
	IdempotencyKey string         `json:"idempotency_key,omitempty"`
	OperationID    string         `json:"operation_id,omitempty"`
}

type edgeWriteArgs struct {
	SpaceID        string         `json:"space_id,omitempty"`
	DomainID       string         `json:"domain_id,omitempty"`
	EdgeID         string         `json:"edge_id,omitempty"`
	FromNodeID     string         `json:"from_node_id"`
	ToNodeID       string         `json:"to_node_id"`
	Labels         []string       `json:"labels,omitempty"`
	Properties     map[string]any `json:"properties,omitempty"`
	Payload        map[string]any `json:"payload,omitempty"`
	Meta           map[string]any `json:"meta,omitempty"`
	IdempotencyKey string         `json:"idempotency_key,omitempty"`
	OperationID    string         `json:"operation_id,omitempty"`
}

type writeTransactionResult struct {
	result           any
	commit           *clientv1.TransactionCommit
	idempotentReplay bool
}

func newCreateNodeTool(a *app) tool {
	return tool{
		Name:        "mycel_create_node",
		Description: "Create a graph node in a read-write transaction. Requires write mode to be enabled in mycel-mcp config.",
		InputSchema: nodeWriteSchema(false),
		Handler: func(ctx context.Context, raw json.RawMessage) (any, error) {
			var in nodeWriteArgs
			if err := decodeArgs(raw, &in); err != nil {
				return nil, err
			}
			node, err := nodeCreateFromArgs(in)
			if err != nil {
				return nil, err
			}
			spaceID, domainID, err := a.requireScope(in.SpaceID, in.DomainID)
			if err != nil {
				return nil, err
			}
			return a.withWriteTransaction(ctx, spaceID, domainID, in.IdempotencyKey, in.OperationID, func(ctx context.Context, client *mycel.Client, txID string) (any, error) {
				callCtx, cancel := client.AuthCallContext(ctx)
				defer cancel()
				res, err := client.Graph.CreateNode(callCtx, &clientv1.CreateNodeRequest{TransactionId: txID, Node: node})
				if err != nil {
					return nil, err
				}
				return res.GetNode(), nil
			})
		},
	}
}

func newUpsertNodeTool(a *app) tool {
	return tool{
		Name:        "mycel_upsert_node",
		Description: "Create or replace/upsert a graph node in a read-write transaction. Requires write mode to be enabled in mycel-mcp config.",
		InputSchema: nodeWriteSchema(true),
		Handler: func(ctx context.Context, raw json.RawMessage) (any, error) {
			var in nodeWriteArgs
			if err := decodeArgs(raw, &in); err != nil {
				return nil, err
			}
			if strings.TrimSpace(in.NodeID) == "" {
				return nil, errors.New("node_id is required for upsert")
			}
			node, err := nodeCreateFromArgs(in)
			if err != nil {
				return nil, err
			}
			spaceID, domainID, err := a.requireScope(in.SpaceID, in.DomainID)
			if err != nil {
				return nil, err
			}
			return a.withWriteTransaction(ctx, spaceID, domainID, in.IdempotencyKey, in.OperationID, func(ctx context.Context, client *mycel.Client, txID string) (any, error) {
				callCtx, cancel := client.AuthCallContext(ctx)
				defer cancel()
				res, err := client.Graph.UpsertNode(callCtx, &clientv1.UpsertNodeRequest{TransactionId: txID, Node: node})
				if err != nil {
					return nil, err
				}
				return res.GetNode(), nil
			})
		},
	}
}

func newCreateEdgeTool(a *app) tool {
	return tool{
		Name:        "mycel_create_edge",
		Description: "Create a graph edge in a read-write transaction. Requires write mode to be enabled in mycel-mcp config.",
		InputSchema: edgeWriteSchema(),
		Handler: func(ctx context.Context, raw json.RawMessage) (any, error) {
			var in edgeWriteArgs
			if err := decodeArgs(raw, &in); err != nil {
				return nil, err
			}
			edge, err := edgeCreateFromArgs(in)
			if err != nil {
				return nil, err
			}
			spaceID, domainID, err := a.requireScope(in.SpaceID, in.DomainID)
			if err != nil {
				return nil, err
			}
			return a.withWriteTransaction(ctx, spaceID, domainID, in.IdempotencyKey, in.OperationID, func(ctx context.Context, client *mycel.Client, txID string) (any, error) {
				callCtx, cancel := client.AuthCallContext(ctx)
				defer cancel()
				res, err := client.Graph.CreateEdge(callCtx, &clientv1.CreateEdgeRequest{TransactionId: txID, Edge: edge})
				if err != nil {
					return nil, err
				}
				return res.GetEdge(), nil
			})
		},
	}
}

func (a *app) withWriteTransaction(ctx context.Context, spaceID, domainID, idempotencyKey, operationID string, fn func(context.Context, *mycel.Client, string) (any, error)) (any, error) {
	if !a.cfg.WriteEnabled {
		return nil, errors.New("write mode is disabled; run `mycel-mcp configure --enable-writes` and restart/reconnect the MCP client")
	}
	spaceID, domainID, err := a.requireScope(spaceID, domainID)
	if err != nil {
		return nil, err
	}
	client, err := a.mycelClient(ctx)
	if err != nil {
		return nil, err
	}
	sessionID, err := client.OpenSession(ctx, spaceID, domainID)
	if err != nil {
		return nil, err
	}
	defer func() { _ = client.CloseSession(context.Background(), sessionID) }()

	tx, err := client.BeginReadWriteTransactionWithOperationID(ctx, sessionID, strings.TrimSpace(operationID))
	if err != nil {
		return nil, err
	}
	txID := tx.GetTransactionId()
	committed := false
	defer func() {
		if !committed {
			_ = client.CloseTransaction(context.Background(), txID)
		}
	}()

	value, err := fn(ctx, client, txID)
	if err != nil {
		return nil, err
	}
	commitOptions := mycel.CommitTransactionOptions{}
	if strings.TrimSpace(idempotencyKey) != "" {
		commitOptions = mycel.CommitTransactionOptions{IdempotencyKey: strings.TrimSpace(idempotencyKey), SpaceID: spaceID, DomainID: domainID}
	}
	commitRes, err := client.CommitTransactionWithOptions(ctx, txID, commitOptions)
	if err != nil {
		return nil, err
	}
	committed = true
	return writeTransactionResult{result: value, commit: commitRes.GetCommit(), idempotentReplay: commitRes.GetIdempotentReplay()}, nil
}

func nodeCreateFromArgs(in nodeWriteArgs) (*clientv1.NodeCreate, error) {
	properties, err := structFromMap(in.Properties, "properties")
	if err != nil {
		return nil, err
	}
	payload, err := structFromMap(in.Payload, "payload")
	if err != nil {
		return nil, err
	}
	meta, err := structFromMap(in.Meta, "meta")
	if err != nil {
		return nil, err
	}
	node := &clientv1.NodeCreate{Labels: cleanStrings(in.Labels), Properties: properties, Payload: payload, Meta: meta}
	if strings.TrimSpace(in.NodeID) != "" {
		id := strings.TrimSpace(in.NodeID)
		node.NodeId = &id
	}
	return node, nil
}

func edgeCreateFromArgs(in edgeWriteArgs) (*clientv1.EdgeCreate, error) {
	from := strings.TrimSpace(in.FromNodeID)
	to := strings.TrimSpace(in.ToNodeID)
	if from == "" || to == "" {
		return nil, errors.New("from_node_id and to_node_id are required")
	}
	properties, err := structFromMap(in.Properties, "properties")
	if err != nil {
		return nil, err
	}
	payload, err := structFromMap(in.Payload, "payload")
	if err != nil {
		return nil, err
	}
	meta, err := structFromMap(in.Meta, "meta")
	if err != nil {
		return nil, err
	}
	edge := &clientv1.EdgeCreate{FromNodeId: from, ToNodeId: to, Labels: cleanStrings(in.Labels), Properties: properties, Payload: payload, Meta: meta}
	if strings.TrimSpace(in.EdgeID) != "" {
		id := strings.TrimSpace(in.EdgeID)
		edge.EdgeId = &id
	}
	return edge, nil
}

func structFromMap(values map[string]any, name string) (*structpb.Struct, error) {
	if len(values) == 0 {
		return nil, nil
	}
	out, err := structpb.NewStruct(values)
	if err != nil {
		return nil, fmt.Errorf("invalid %s: %w", name, err)
	}
	return out, nil
}

func protoJSONAny(msg proto.Message) (any, error) {
	if msg == nil {
		return nil, nil
	}
	data, err := protojson.MarshalOptions{UseProtoNames: true, EmitUnpopulated: false}.Marshal(msg)
	if err != nil {
		return nil, err
	}
	var out any
	if err := json.Unmarshal(data, &out); err != nil {
		return nil, err
	}
	return out, nil
}

func writeResultJSON(value writeTransactionResult) (map[string]any, error) {
	msg, ok := value.result.(proto.Message)
	if !ok {
		return nil, fmt.Errorf("write result has unexpected type %T", value.result)
	}
	result, err := protoJSONAny(msg)
	if err != nil {
		return nil, err
	}
	commit, err := protoJSONAny(value.commit)
	if err != nil {
		return nil, err
	}
	return map[string]any{"result": result, "commit": commit, "idempotent_replay": value.idempotentReplay}, nil
}

func nodeWriteSchema(upsert bool) map[string]any {
	props := commonWriteProperties()
	props["node_id"] = stringSchema("Optional client-supplied node ID. Required for upsert.")
	props["labels"] = arrayStringSchema("Node labels.")
	props["properties"] = objectFreeformSchema("Node properties as a JSON object.")
	props["payload"] = objectFreeformSchema("Node payload as a JSON object.")
	props["meta"] = objectFreeformSchema("Node metadata as a JSON object.")
	required := []string{}
	if upsert {
		required = []string{"node_id"}
	}
	return objectSchema(props, required)
}

func edgeWriteSchema() map[string]any {
	props := commonWriteProperties()
	props["edge_id"] = stringSchema("Optional client-supplied edge ID.")
	props["from_node_id"] = stringSchema("Source/from node ID.")
	props["to_node_id"] = stringSchema("Target/to node ID.")
	props["labels"] = arrayStringSchema("Edge labels.")
	props["properties"] = objectFreeformSchema("Edge properties as a JSON object.")
	props["payload"] = objectFreeformSchema("Edge payload as a JSON object.")
	props["meta"] = objectFreeformSchema("Edge metadata as a JSON object.")
	return objectSchema(props, []string{"from_node_id", "to_node_id"})
}

func commonWriteProperties() map[string]any {
	return map[string]any{
		"space_id":        stringSchema("Space ID. Defaults to MYCEL_MCP_DEFAULT_SPACE_ID or configured default when omitted."),
		"domain_id":       stringSchema("Domain ID. Defaults to MYCEL_MCP_DEFAULT_DOMAIN_ID or configured default when omitted."),
		"idempotency_key": stringSchema("Optional durable commit idempotency key. Must be non-secret and at most 128 bytes."),
		"operation_id":    stringSchema("Optional operation correlation ID."),
	}
}
