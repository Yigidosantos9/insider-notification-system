CREATE TABLE templates (
    id         uuid PRIMARY KEY,
    name       text NOT NULL UNIQUE,
    channel    channel NOT NULL,
    body       text NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now()
);

-- Which template (if any) produced a notification's content; useful for analytics.
ALTER TABLE notifications ADD COLUMN template_id uuid REFERENCES templates (id) ON DELETE SET NULL;
