package event

import "github.com/neutrinocorp/streamhub"

type ProductPaid struct {
	ID          string `json:"product_id" avro:"product_id"`
	SKU         string `json:"product_sku" avro:"product_sku"`
	DisplayName string `json:"display_name" avro:"display_name"`
	Quantity    int    `json:"quantity" avro:"quantity"`
	OrderedAt   string `json:"ordered_at" avro:"ordered_at"`
	PaidAt      string `json:"paid_at" avro:"paid_at"`
}

var _ streamhub.Event = ProductPaid{}

func (p ProductPaid) Subject() string {
	return p.ID
}
