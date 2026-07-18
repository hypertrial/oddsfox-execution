ALTER TABLE risk_reservations
ADD COLUMN quote_exposure TEXT NOT NULL DEFAULT '0';

ALTER TABLE orders
ADD COLUMN quote_exposure TEXT NOT NULL DEFAULT '0';
