// Package proxy implementa il Client Proxy Service.
//
// Il Proxy è il punto di ingresso stateless per i client esterni. Non mantiene
// dati applicativi e non partecipa al protocollo di consenso: il suo compito è
// instradare Put, Get, Delete e GetLeader verso il Consensus Node corretto.
//
// Il package implementa Client-side Service Discovery: il leader non è
// configurato staticamente nel client, ma viene scoperto dinamicamente dal Proxy
// interrogando i nodi o usando i leader_hint restituiti dai follower.
//
// Il package integra inoltre retry, backoff e Circuit Breaker per evitare che
// nodi non disponibili causino cascading failure o degradino l'intero sistema.

package proxy

import (
	"context"
	"fmt"
	"math"
	"math/rand"
	"time"

	kvpb "github.com/enricobarbatano/Progetto-Sistemi-Distribuiti-e-Cloud-Computing/gen/go/kvpb"
)

// Router coordina l'instradamento delle richieste client verso il cluster.
//
// Il Router mantiene solo stato operativo temporaneo, come la cache del leader,
// e non contiene dati applicativi key-value. La sua responsabilità è rendere il
// cluster trasparente al client: il client invia sempre le richieste al Proxy,
// mentre il Router decide a quale Consensus Node inoltrarle.
//
// Il Router combina tre meccanismi: cache del leader, Client-side Service
// Discovery e Circuit Breaker per nodo.

type Router struct {
	config Config

	// leaderCache conserva l'ultimo leader noto.
	//
	// La cache evita di eseguire una discovery completa a ogni richiesta. Viene
	// aggiornata tramite GetLeader o tramite leader_hint restituiti dai follower.
	cache  *LeaderCache
	client *NodeClient
	// cbManager contiene un Circuit Breaker indipendente per ogni nodo.
	//
	// In questo modo il fallimento di un singolo nodo non blocca le richieste verso
	// gli altri peer disponibili.
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

// discoverLeader cerca il leader corrente interrogando i nodi conosciuti.
//
// Questa funzione viene usata quando la cache del leader è vuota, scaduta o
// ritenuta non più valida dopo un errore. Il primo nodo che dichiara un leader
// valido permette al Router di aggiornare la cache.
//
// La discovery è client-side perché avviene dentro il Proxy, non tramite un
// servizio esterno di registrazione.
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

// Put inoltra una richiesta di scrittura al leader del cluster.
//
// Il Proxy non applica direttamente la scrittura e non modifica alcuno stato
// key-value locale. La richiesta viene inoltrata al Consensus Node leader, che
// la inserisce nel log e la conferma solo dopo replica su quorum.
//
// Se il nodo contattato non è leader, il leader_hint restituito viene usato per
// aggiornare la cache e riprovare verso il nodo corretto.
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

// Get inoltra una richiesta di lettura al leader del cluster.
//
// Anche le letture passano dal leader, così il sistema evita di servire valori
// obsoleti da follower non aggiornati. Il Proxy mantiene trasparente questa
// scelta al client, che continua a usare un singolo endpoint.
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

// Delete inoltra una richiesta di cancellazione al leader del cluster.
//
// Come Put, anche Delete è una scrittura logica: viene serializzata come entry
// del log dal leader e diventa visibile solo dopo commit su quorum.
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
