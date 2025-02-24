package main

// Item – структура, описывающая продукт в заказе
type Item struct {
	ProductID string  `json:"product_id"`
	Quantity  int     `json:"quantity"`
	Price     float64 `json:"price"`
}

// Order – структура, описывающая заказ с продуктами
type Order struct {
	Offset     int     `json:"offset"`
	OrderID    string  `json:"order_id"`
	UserID     string  `json:"user_id"`
	Items      []Item  `json:"items"`
	TotalPrice float64 `json:"total_price"`
}
