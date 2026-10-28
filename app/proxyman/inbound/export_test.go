package inbound

import "github.com/xtls/xray-core/common/singmux"

// DomainSocketH2MuxOptions reports the H2MUX server options of the handler's Unix socket workers.
func DomainSocketH2MuxOptions(h *AlwaysOnInboundHandler) []singmux.H2MuxOptions {
	var options []singmux.H2MuxOptions
	for _, worker := range h.workers {
		if w, ok := worker.(*dsWorker); ok {
			options = append(options, w.h2mux)
		}
	}
	return options
}
