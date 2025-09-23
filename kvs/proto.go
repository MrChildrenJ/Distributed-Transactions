package kvs

type PutRequest struct {
	Key      string
	Value    string
	ClientID uint64
	Txid     uint64
}

type PutResponse struct {
	Success bool // false if lock acquisition failed
}

type GetRequest struct {
	Key      string
	ClientID uint64
	Txid     uint64
}

type GetResponse struct {
	Value   string
	Success bool // false if lock acquisition failed
}

type CommitRequest struct {
	ClientID uint64
	Txid     uint64
}

type CommitResponse struct {
	Success bool // false if commit failed
}

type AbortRequest struct {
	ClientID uint64
	Txid     uint64
}

type AbortResponse struct {
}
