// Package proxy contiene la logica interna del Client Proxy Service.
//
// Questo file contiene Router, il componente che decide verso quale Consensus
// Node inoltrare una richiesta.
//
// Router coordina:
//   - Config, per conoscere seed nodes, timeout, retry e backoff;
//   - LeaderCache, per ricordare il leader noto;
//   - NodeClient, per eseguire chiamate gRPC verso i nodi;
//   - CircuitBreakerManager, per evitare chiamate ripetute verso nodi guasti.
//
// Router non espone direttamente un server gRPC: quello sarà responsabilità
// di service.go. Router non salva dati applicativi: resta stateless rispetto
// alla key-value map del cluster.
package proxy

import (
	"context"
	"fmt"
	"math"
	"math/rand"
	"time"

	kvpb "github.com/enricobarbatano/Progetto-Sistemi-Distribuiti-e-Cloud-Computing/gen/go/kvpb"
)

// Router instrada le richieste client verso il leader del cluster.
//
// Il Router mantiene solo stato operativo leggero, come la cache del leader.
// Non contiene dati applicativi e non partecipa al consenso Raft.
type Router struct {
	config   Config
	cache    *LeaderCache
	client   *NodeClient
	breakers *CircuitBreakerManager
}

// NewRouter costruisce un nuovo Router del Client Proxy.
//
// I componenti vengono passati esplicitamente per evitare una classe monolitica.
// Questo rende il Router testabile e mantiene separate le responsabilità.
func NewRouter(config Config, cache *LeaderCache, client *NodeClient, breakers *CircuitBreakerManager) *Router {
	return &Router{
		config:   config,
		cache:    cache,
		client:   client,
		breakers: breakers,
	}
}

// DiscoverLeader interroga i seed nodes finché non trova un leader noto.
//
// Se un nodo risponde con has_leader=true, il Router aggiorna la LeaderCache e
// restituisce le informazioni del leader. Le chiamate sono protette dal Circuit
// Breaker del nodo interrogato.
func (r *Router) DiscoverLeader(ctx context.Context) (LeaderInfo, error) {
	var lastErr error

	for _, address := range r.config.ConsensusNodes {
		response, err := r.executeWithBreaker(ctx, address, func(callCtx context.Context) (any, error) {
			return r.client.GetLeader(callCtx, address)
		})
		if err != nil {
			lastErr = err
			continue
		}

		leaderResp, ok := response.(*kvpb.GetLeaderResponse)
		if !ok {
			lastErr = fmt.Errorf("unexpected GetLeader response type from %s", address)
			continue
		}

		if leaderResp.HasLeader && leaderResp.LeaderAddress != "" {
			info := LeaderInfo{
				ID:      leaderResp.LeaderId,
				Address: leaderResp.LeaderAddress,
				Term:    leaderResp.Term,
			}
			r.cache.Set(info)
			return info, nil
		}
	}

	if lastErr != nil {
		return LeaderInfo{}, fmt.Errorf("cannot discover leader: %w", lastErr)
	}

	return LeaderInfo{}, fmt.Errorf("cannot discover leader: no seed node reported a leader")
}

// LeaderInfo restituisce il leader noto oppure lo scopre interrogando i seed nodes.
//
// Questo metodo viene usato anche da ProxyService.GetLeader per restituire al
// client una risposta più completa con leader_id e term quando disponibili.
func (r *Router) LeaderInfo(ctx context.Context) (LeaderInfo, error) {
	return r.leaderOrDiscover(ctx)
}

// Put inoltra una richiesta Put al leader corrente.
//
// Se il leader non è noto, lo scopre con DiscoverLeader.
// Se la risposta contiene leader_hint, aggiorna la cache e ritenta.
func (r *Router) Put(ctx context.Context, key string, value string) (*kvpb.PutResponse, error) {
	var lastResp *kvpb.PutResponse
	var lastErr error

	for attempt := 0; attempt <= r.config.MaxRetries; attempt++ {
		leader, err := r.leaderOrDiscover(ctx)
		if err != nil {
			lastErr = err
			r.sleepBeforeRetry(attempt)
			continue
		}

		response, err := r.executeWithBreaker(ctx, leader.Address, func(callCtx context.Context) (any, error) {
			return r.client.Put(callCtx, leader.Address, key, value)
		})
		if err != nil {
			lastErr = err
			r.cache.Clear()
			r.sleepBeforeRetry(attempt)
			continue
		}

		putResp, ok := response.(*kvpb.PutResponse)
		if !ok {
			lastErr = fmt.Errorf("unexpected Put response type from %s", leader.Address)
			r.cache.Clear()
			r.sleepBeforeRetry(attempt)
			continue
		}

		lastResp = putResp

		if putResp.Success {
			return putResp, nil
		}

		if putResp.LeaderHint != "" {
			r.cache.UpdateFromHint(putResp.LeaderHint)
			r.sleepBeforeRetry(attempt)
			continue
		}

		return putResp, nil
	}

	if lastResp != nil {
		return lastResp, nil
	}

	return nil, lastErr
}

// Get inoltra una richiesta Get al leader corrente.
//
// Anche le letture passano dal leader, perché il cluster attuale non serve
// letture dai follower per evitare dati stale.
func (r *Router) Get(ctx context.Context, key string) (*kvpb.GetResponse, error) {
	var lastResp *kvpb.GetResponse
	var lastErr error

	for attempt := 0; attempt <= r.config.MaxRetries; attempt++ {
		leader, err := r.leaderOrDiscover(ctx)
		if err != nil {
			lastErr = err
			r.sleepBeforeRetry(attempt)
			continue
		}

		response, err := r.executeWithBreaker(ctx, leader.Address, func(callCtx context.Context) (any, error) {
			return r.client.Get(callCtx, leader.Address, key)
		})
		if err != nil {
			lastErr = err
			r.cache.Clear()
			r.sleepBeforeRetry(attempt)
			continue
		}

		getResp, ok := response.(*kvpb.GetResponse)
		if !ok {
			lastErr = fmt.Errorf("unexpected Get response type from %s", leader.Address)
			r.cache.Clear()
			r.sleepBeforeRetry(attempt)
			continue
		}

		lastResp = getResp

		if getResp.Error == "" {
			return getResp, nil
		}

		if getResp.LeaderHint != "" {
			r.cache.UpdateFromHint(getResp.LeaderHint)
			r.sleepBeforeRetry(attempt)
			continue
		}

		return getResp, nil
	}

	if lastResp != nil {
		return lastResp, nil
	}

	return nil, lastErr
}

// Delete inoltra una richiesta Delete al leader corrente.
//
// Come Put, Delete viene accettata solo dal leader e confermata dal cluster
// dopo replica su quorum.
func (r *Router) Delete(ctx context.Context, key string) (*kvpb.DeleteResponse, error) {
	var lastResp *kvpb.DeleteResponse
	var lastErr error

	for attempt := 0; attempt <= r.config.MaxRetries; attempt++ {
		leader, err := r.leaderOrDiscover(ctx)
		if err != nil {
			lastErr = err
			r.sleepBeforeRetry(attempt)
			continue
		}

		response, err := r.executeWithBreaker(ctx, leader.Address, func(callCtx context.Context) (any, error) {
			return r.client.Delete(callCtx, leader.Address, key)
		})
		if err != nil {
			lastErr = err
			r.cache.Clear()
			r.sleepBeforeRetry(attempt)
			continue
		}

		deleteResp, ok := response.(*kvpb.DeleteResponse)
		if !ok {
			lastErr = fmt.Errorf("unexpected Delete response type from %s", leader.Address)
			r.cache.Clear()
			r.sleepBeforeRetry(attempt)
			continue
		}

		lastResp = deleteResp

		if deleteResp.Success {
			return deleteResp, nil
		}

		if deleteResp.LeaderHint != "" {
			r.cache.UpdateFromHint(deleteResp.LeaderHint)
			r.sleepBeforeRetry(attempt)
			continue
		}

		return deleteResp, nil
	}

	if lastResp != nil {
		return lastResp, nil
	}

	return nil, lastErr
}

// leaderOrDiscover restituisce il leader dalla cache o lo scopre interrogando
// i seed nodes.
func (r *Router) leaderOrDiscover(ctx context.Context) (LeaderInfo, error) {
	if leader, ok := r.cache.Get(); ok {
		return leader, nil
	}

	return r.DiscoverLeader(ctx)
}

// executeWithBreaker esegue una chiamata verso un nodo usando timeout RPC e
// Circuit Breaker.
//
// Ogni chiamata riceve un contesto con timeout derivato dal contesto originale.
// Se il Circuit Breaker è aperto, la richiesta fallisce subito.
func (r *Router) executeWithBreaker(ctx context.Context, address string, call func(callCtx context.Context) (any, error)) (any, error) {
	callCtx, cancel := context.WithTimeout(ctx, r.config.RPCTimeout)
	defer cancel()

	return r.breakers.Execute(address, func() (any, error) {
		return call(callCtx)
	})
}

// sleepBeforeRetry applica exponential backoff con jitter.
//
// Il delay cresce esponenzialmente in base al numero di attempt e viene limitato
// da MaxBackoff. Il jitter evita che molte richieste ritentino tutte nello
// stesso istante dopo un errore temporaneo.
func (r *Router) sleepBeforeRetry(attempt int) {
	if r.config.Backoff <= 0 {
		return
	}

	if attempt >= r.config.MaxRetries {
		return
	}

	delay := r.exponentialBackoffDelay(attempt)
	time.Sleep(delay)
}

func (r *Router) exponentialBackoffDelay(attempt int) time.Duration {
	multiplier := math.Pow(2, float64(attempt))
	delay := time.Duration(float64(r.config.Backoff) * multiplier)

	if r.config.MaxBackoff > 0 && delay > r.config.MaxBackoff {
		delay = r.config.MaxBackoff
	}

	if r.config.JitterRatio <= 0 || delay <= 0 {
		return delay
	}

	maxJitter := time.Duration(float64(delay) * float64(r.config.JitterRatio) / 100.0)
	if maxJitter <= 0 {
		return delay
	}

	jitter := time.Duration(rand.Int63n(int64(maxJitter) + 1))

	return delay + jitter
}
