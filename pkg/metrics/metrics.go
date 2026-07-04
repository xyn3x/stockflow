package metrics 

import(
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

type Metrics struct {
	EventsTotal 	*prometheus.CounterVec 
	EventsDropped 	*prometheus.CounterVec
	ProcessingTime 	*prometheus.HistogramVec
	WSClients		prometheus.Gauge 
	NATSLag			prometheus.Gauge
}

func New(service string) *Metrics {
	
	return &Metrics {
		EventsTotal: promauto.NewCounterVec(prometheus.CounterOpts{
			Namespace: 	"stockflow", 
			Name: 		"events_total",
			Help: 		"Total number of handled events.",
		}, []string {"service", "type", "status"}),

		EventsDropped: promauto.NewCounterVec(prometheus.CounterOpts{
			Namespace: 	"stockflow",
			Name:		"events_dropped_total",
			Help:		"Total number of dropped events.",
		}, []string {"service", "reason"}),
		
		ProcessingTime: promauto.NewHistogramVec(prometheus.HistogramOpts{
			Namespace:	"stockflow",
			Name:		"processing_duration_seconds",
			Help:		"Time spent processing an event.",
			Buckets:	prometheus.DefBuckets, 
		}, []string {"service", "type"}),

		WSClients: promauto.NewGauge(prometheus.GaugeOpts{
			Namespace:		"stockflow",
			Name:			"ws_clients_connected",
			Help:			"Number of connected WS clients.", 
			ConstLabels:	prometheus.Labels{"service": service}, 
		}),

		NATSLag: promauto.NewGauge(prometheus.GaugeOpts{
			Namespace:		"stockflow",
			Name:			"nats_consumer_lag",
			Help:			"Pending messages in NATS consumer.",
			ConstLabels:	prometheus.Labels{"service": service},
		}),
	}
}

func Handler() http.Handler {
	return promhttp.Handler()
}