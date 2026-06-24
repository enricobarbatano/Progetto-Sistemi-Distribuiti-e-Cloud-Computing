// Package proxy contiene la logica interna del Client Proxy Service.
//
// Questo file contiene ProxyService, cioè l'implementazione gRPC esposta ai
// client esterni.
//
// ProxyService non conosce i dettagli del cluster Raft: non apre connessioni
// verso i Consensus Node, non gestisce leader cache e non implementa retry.
// Tutta la logica di routing viene delegata a Router.
package proxy

import (
	"context"

	kvpb "github.com/enricobarbatano/Progetto-Sistemi-Distribuiti-e-Cloud-Computing/gen/go/kvpb"
)

// ProxyService implementa il servizio KeyValueService esposto dal Client Proxy.
//
// Il suo ruolo è ricevere richieste dai client e inoltrarle al Router.
// In questo modo il Proxy rimane un punto di ingresso unico e stateless
// rispetto ai dati applicativi.
type ProxyService struct {
	kvpb.UnimplementedKeyValueServiceServer

	router *Router
}

// NewProxyService costruisce il servizio gRPC del Proxy.
//
// Il Router viene iniettato dall'esterno per mantenere separata la logica
// di trasporto gRPC dalla logica di routing verso il cluster.
func NewProxyService(router *Router) *ProxyService {
	return &ProxyService{
		router: router,
	}
}

// Put riceve una richiesta di scrittura dal client esterno.
//
// La richiesta viene delegata al Router, che scopre il leader, applica
// Circuit Breaker e retry, e inoltra la Put al Consensus Node corretto.
func (s *ProxyService) Put(ctx context.Context, req *kvpb.PutRequest) (*kvpb.PutResponse, error) {
	return s.router.Put(ctx, req.Key, req.Value)
}

// Get riceve una richiesta di lettura dal client esterno.
//
// Anche le letture vengono inoltrate al leader tramite Router, perché il cluster
// attuale evita letture dai follower per non restituire dati stale.
func (s *ProxyService) Get(ctx context.Context, req *kvpb.GetRequest) (*kvpb.GetResponse, error) {
	return s.router.Get(ctx, req.Key)
}

// Delete riceve una richiesta di cancellazione dal client esterno.
//
// Come Put, Delete viene inoltrata al leader e confermata dal cluster solo dopo
// replica su quorum.
func (s *ProxyService) Delete(ctx context.Context, req *kvpb.DeleteRequest) (*kvpb.DeleteResponse, error) {
	return s.router.Delete(ctx, req.Key)
}

// GetLeader espone al client esterno il leader attualmente noto al Proxy.
//
// Se il Router non ha ancora un leader in cache, esegue una discovery sui seed
// nodes. Questa RPC è utile per debug e test manuali.
func (s *ProxyService) GetLeader(ctx context.Context, req *kvpb.GetLeaderRequest) (*kvpb.GetLeaderResponse, error) {
	leader, err := s.router.LeaderInfo(ctx)
	if err != nil {
		return &kvpb.GetLeaderResponse{
			HasLeader: false,
		}, nil
	}

	return &kvpb.GetLeaderResponse{
		HasLeader:     true,
		LeaderId:      leader.ID,
		LeaderAddress: leader.Address,
		Term:          leader.Term,
	}, nil
}
