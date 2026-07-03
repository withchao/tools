package inprocess

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/openimsdk/protocol/constant"
	"github.com/openimsdk/tools/errs"
	"github.com/openimsdk/tools/mcontext"
)

const BroadcastPath = "/internal/api_invoke_rpc"

// BroadcastAddressProvider returns the http api addresses of the other
// in-process-mode instances, excluding this one; the local instance is always
// invoked in memory.
type BroadcastAddressProvider func(ctx context.Context) ([]string, error)

// BroadcastAddressFunc is kept as a source-compatible alias for older callers.
type BroadcastAddressFunc = BroadcastAddressProvider

// remoteInvokeTimeout caps a cross-instance invoke end to end; it must exceed
// methodWaitTimeout so a peer still waiting for late method registration can
// answer with its own timeout error before this client gives up.
const remoteInvokeTimeout = time.Second * 15

var remoteInvokeClient = &http.Client{Timeout: remoteInvokeTimeout}

func newRemoteClientConn(addr, serviceName, secret string) *remoteClientConn {
	endpoint, target := parseRemoteAddress(addr)
	return &remoteClientConn{
		endpoint:    endpoint,
		serviceName: serviceName,
		secret:      secret,
		target:      target,
		codec:       newProtoCodec(),
	}
}

func parseRemoteAddress(addr string) (endpoint, target string) {
	addr = strings.TrimSpace(addr)
	if u, err := url.Parse(addr); err == nil && u.Scheme != "" && u.Host != "" {
		return strings.TrimRight(addr, "/") + BroadcastPath, u.Host
	}
	return "http://" + addr + BroadcastPath, addr
}

// remoteClientConn is a grpc.ClientConnInterface that forwards unary calls to
// another in-process-mode instance through its http api RpcInvoke endpoint.
type remoteClientConn struct {
	endpoint    string
	serviceName string
	secret      string
	target      string
	codec       messageCodec
}

// invokeRequest must match the request type of internalApi.RpcInvoke.
type invokeRequest struct {
	Service string `json:"service"`
	Method  string `json:"method"`
	Secret  string `json:"secret"`
	Request []byte `json:"request"`
}

// invokeResponse is the apiresp.ApiResponse envelope with []byte data.
type invokeResponse struct {
	ErrCode int    `json:"errCode"`
	ErrMsg  string `json:"errMsg"`
	ErrDlt  string `json:"errDlt"`
	Data    []byte `json:"data"`
}

func (c *remoteClientConn) Target() string {
	return c.target
}

func (c *remoteClientConn) Invoke(ctx context.Context, method string, args any, reply any, _ ...grpc.CallOption) error {
	reqData, err := c.codec.Marshal(args)
	if err != nil {
		return err
	}
	body, err := json.Marshal(invokeRequest{
		Service: c.serviceName,
		Method:  method,
		Secret:  c.secret,
		Request: reqData,
	})
	if err != nil {
		return errs.WrapMsg(err, "marshal remote invoke request")
	}
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, c.endpoint, bytes.NewReader(body))
	if err != nil {
		return errs.WrapMsg(err, "new remote invoke request", "url", c.endpoint)
	}
	httpReq.Header.Set("Content-Type", "application/json")
	operationID := mcontext.GetOperationID(ctx)
	if operationID == "" {
		operationID = fmt.Sprintf("inprocess_remote_%d", time.Now().UnixNano())
	}
	httpReq.Header.Set(constant.OperationID, operationID)
	httpResp, err := remoteInvokeClient.Do(httpReq)
	if err != nil {
		return errs.WrapMsg(err, "do remote invoke request", "url", c.endpoint, "method", method)
	}
	defer httpResp.Body.Close()
	respBody, err := io.ReadAll(httpResp.Body)
	if err != nil {
		return errs.WrapMsg(err, "read remote invoke response", "url", c.endpoint, "method", method)
	}
	if httpResp.StatusCode != http.StatusOK {
		return errs.New("remote invoke http status not ok", "url", c.endpoint, "method", method, "status", httpResp.Status, "body", string(respBody)).Wrap()
	}
	var resp invokeResponse
	if err := json.Unmarshal(respBody, &resp); err != nil {
		return errs.WrapMsg(err, "unmarshal remote invoke response", "url", c.endpoint, "method", method, "body", string(respBody))
	}
	if resp.ErrCode != 0 {
		codeErr := errs.NewCodeError(resp.ErrCode, resp.ErrMsg)
		if resp.ErrDlt != "" {
			codeErr = codeErr.WithDetail(resp.ErrDlt)
		}
		return codeErr.Wrap()
	}
	return c.codec.Unmarshal(resp.Data, reply)
}

func (c *remoteClientConn) NewStream(context.Context, *grpc.StreamDesc, string, ...grpc.CallOption) (grpc.ClientStream, error) {
	return nil, status.Errorf(codes.Unimplemented, "method stream not implemented")
}
