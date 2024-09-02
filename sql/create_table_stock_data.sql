CREATE TABLE IF NOT EXISTS stock_data
(
    stock_ticker text NOT NULL,
    "timestamp" timestamp with time zone NOT NULL,
    price double precision NOT NULL,
    CONSTRAINT stock_data_pkey PRIMARY KEY (stock_ticker, "timestamp")
)

