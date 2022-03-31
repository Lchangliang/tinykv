package server

import (
	"context"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// The functions below are Server's Raw API. (implements TinyKvServer).
// Some helper methods can be found in sever.go in the current directory

// RawGet return the corresponding Get response based on RawGetRequest's CF and Key fields
func (server *Server) RawGet(_ context.Context, req *kvrpcpb.RawGetRequest) (*kvrpcpb.RawGetResponse, error) {
	resp := &kvrpcpb.RawGetResponse{}
	reader, err := server.storage.Reader(nil)
	defer reader.Close()
	if err != nil {
		resp.Error = err.Error()
		return resp, err
	}
	value, err := reader.GetCF(req.GetCf(), req.GetKey())
	if value == nil {
		resp.NotFound = true
		return resp, nil
	}
	resp.Value = value
	return resp, nil
}

func (server *Server) RawPut(_ context.Context, req *kvrpcpb.RawPutRequest) (*kvrpcpb.RawPutResponse, error) {
	resp := &kvrpcpb.RawPutResponse{}
	put := storage.Put{
		Key:   req.Key,
		Value: req.Value,
		Cf:    req.Cf,
	}
	var batch []storage.Modify
	batch = append(batch, storage.Modify{
		Data: put,
	})
	err := server.storage.Write(nil, batch)
	if err != nil {
		resp.Error = err.Error()
		return resp, err
	}
	return resp, nil
}

func (server *Server) RawDelete(_ context.Context, req *kvrpcpb.RawDeleteRequest) (*kvrpcpb.RawDeleteResponse, error) {
	resp := &kvrpcpb.RawDeleteResponse{}
	delete := storage.Delete{
		Key: req.Key,
		Cf:  req.Cf,
	}
	var batch []storage.Modify
	batch = append(batch, storage.Modify{
		Data: delete,
	})
	err := server.storage.Write(nil, batch)
	if err != nil {
		resp.Error = err.Error()
		return resp, err
	}
	return resp, nil
}

func (server *Server) RawScan(_ context.Context, req *kvrpcpb.RawScanRequest) (*kvrpcpb.RawScanResponse, error) {
	resp := &kvrpcpb.RawScanResponse{}
	reader, err := server.storage.Reader(nil)
	defer reader.Close()
	if err != nil {
		resp.Error = err.Error()
		return resp, err
	}
	iterator := reader.IterCF(req.GetCf())
	defer iterator.Close()
	if req.StartKey != nil {
		iterator.Seek(req.StartKey)
	}
	var i uint32 = 0
	for ; i < req.Limit; i++ {
		if !iterator.Valid() {
			break
		}
		item := iterator.Item()
		value, _ := item.Value()
		kvPair := &kvrpcpb.KvPair{
			Key:   item.Key(),
			Value: value,
		}
		resp.Kvs = append(resp.Kvs, kvPair)
		iterator.Next()
	}
	return resp, nil
}
