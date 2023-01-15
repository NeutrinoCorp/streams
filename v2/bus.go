package streams

type Bus struct {
	Publisher
	*SubscriberSupervisor
	*StreamRegistry
}

func NewBus(w Writer, r Reader) Bus {
	reg := NewStreamRegistry()
	return Bus{
		Publisher: NewPublisher(
			WithWriter(w),
			WithRegistry(reg)),
		SubscriberSupervisor: NewSubscriberSupervisor(reg, r),
		StreamRegistry:       reg,
	}
}
