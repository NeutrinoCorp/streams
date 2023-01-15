package streams

type Bus struct {
	Publisher
	*SubscriberSupervisor
	*StreamRegistry
}

func NewBus(w Writer, r Reader, opts ...BusOption) Bus {
	baseCfg := busOpts{
		publisherOpts: newDefaultPublisherOpts(),
	}
	for _, o := range opts {
		o.applyBus(&baseCfg)
	}

	reg := NewStreamRegistry()
	b := Bus{
		Publisher: Publisher{
			writer:        w,
			codec:         baseCfg.codec,
			idFactoryFunc: baseCfg.idFactory,
			reg:           reg,
		},
		SubscriberSupervisor: NewSubscriberSupervisor(reg, r),
		StreamRegistry:       reg,
	}
	return b
}
